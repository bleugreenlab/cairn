//! Project types.

use cairn_common::executor_protocol::ExecutorSelector;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HumanBaseMergePolicyState {
    Disabled,
    Enabled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "availability", rename_all = "snake_case")]
pub enum HumanBaseMergePolicyReadState {
    Available { policy: HumanBaseMergePolicy },
    Unavailable { reason: HumanBaseMergePolicyError },
}

impl HumanBaseMergePolicyReadState {
    pub fn from_stored(value: &str) -> Self {
        match HumanBaseMergePolicy::parse(value) {
            Ok(policy) => Self::Available { policy },
            Err(reason) => Self::Unavailable { reason },
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HumanBaseMergePolicy {
    pub version: u32,
    pub state: HumanBaseMergePolicyState,
}

impl HumanBaseMergePolicy {
    pub const DISABLED_V1: Self = Self {
        version: 1,
        state: HumanBaseMergePolicyState::Disabled,
    };

    pub fn parse(value: &str) -> Result<Self, HumanBaseMergePolicyError> {
        let policy: Self =
            serde_json::from_str(value).map_err(|_| HumanBaseMergePolicyError::InvalidEncoding)?;
        if policy.version != 1 {
            return Err(HumanBaseMergePolicyError::UnsupportedVersion(
                policy.version,
            ));
        }
        Ok(policy)
    }

    pub fn is_enabled(self) -> bool {
        self.state == HumanBaseMergePolicyState::Enabled
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "reason", content = "version", rename_all = "snake_case")]
pub enum HumanBaseMergePolicyError {
    InvalidEncoding,
    UnsupportedVersion(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub key: String,
    pub repo_path: String,
    pub context: String,
    pub docs_enabled: bool,
    pub default_branch: String,
    pub human_base_merge_policy: HumanBaseMergePolicyReadState,
    pub next_issue_number: i32,
    pub setup_commands: Option<String>,
    pub terminal_commands: Option<String>,
    /// Background-testing checks as JSON (map of check name → CheckCommand).
    pub checks: Option<String>,
    /// Build-cell populate config as JSON (the copy pattern list).
    pub materialization_populate: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    /// Whether this project is hidden from the sidebar.
    pub hidden: bool,
    /// Whether this project lives in a team replica.
    pub is_team: bool,
    /// Human-readable name of the owning team, resolved from the private team
    /// registry. `None` for local projects or an unregistered team.
    pub team_name: Option<String>,
    /// Whether this machine has a usable local git repository for the project.
    pub repo_cloned: bool,
    /// Whether this project represents the Cairn workspace config root.
    pub is_workspace: bool,
    /// Checks `.cairn/config.yaml` declares that this build cannot run, keyed by
    /// check name. They are absent from `checks`, never planned, and kept in the
    /// file by every settings save.
    #[serde(default)]
    pub unsupported_checks: BTreeMap<String, UnsupportedCheck>,
}

/// The platform families a check's `verdictPlatforms` may name.
pub const VERDICT_PLATFORMS: &[&str] = &["macos", "linux", "windows"];

fn deserialize_verdict_platforms<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let Some(declared) = Option::<Vec<String>>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let mut platforms = Vec::with_capacity(declared.len());
    for value in declared {
        let platform = value.trim().to_ascii_lowercase();
        if platform.is_empty() {
            return Err(D::Error::custom(
                "verdictPlatforms entries must not be blank",
            ));
        }
        if !VERDICT_PLATFORMS.contains(&platform.as_str()) {
            return Err(D::Error::custom(format!(
                "unsupported verdict platform {value:?}; expected macos, linux, or windows"
            )));
        }
        platforms.push(platform);
    }
    platforms.sort();
    platforms.dedup();
    Ok(Some(platforms))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRemoteStatus {
    pub has_remote: bool,
    pub remote_url: Option<String>,
    pub is_workspace: bool,
}

/// Terminal shortcut command configuration.
///
/// The repository declares only a name and exact command. Whether the command
/// may run outside the worktree fence is a separate user-owned setting.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TerminalCommand {
    pub name: String,
    pub command: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateProject {
    /// Optional ID to use instead of generating a new UUID. Set when a project's
    /// row must adopt a specific UUID (e.g. a team-synced project created from an
    /// existing record).
    pub id: Option<String>,
    pub name: String,
    pub key: String,
    pub repo_path: String,
    /// Routes the project to a team's synced database (CAIRN-2132). `None` (the
    /// default for every existing caller) creates a local project whose data
    /// lives in the private database; `Some(team_id)` writes the `projects` row
    /// to that team's already-open replica and records a `project_routes` stub.
    /// Seeded by tests/settings this slice; the api/ control plane populates it
    /// later.
    #[serde(default)]
    pub team_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProject {
    pub id: String,
    pub setup_commands: Option<Vec<String>>,
    pub terminal_commands: Option<Vec<TerminalCommand>>,
    /// Background-testing checks keyed by name. An empty map clears all checks
    /// from `.cairn/config.yaml`; `None` leaves the existing checks untouched.
    pub checks: Option<HashMap<String, CheckCommand>>,
    pub materialization_populate: Option<PopulateConfig>,
    pub default_branch: Option<String>,
    pub human_base_merge_policy: Option<HumanBaseMergePolicy>,
}

#[cfg(test)]
mod human_base_merge_policy_tests {
    use super::*;

    #[test]
    fn policy_is_closed_versioned_and_explicitly_disabled() {
        let encoded = serde_json::to_string(&HumanBaseMergePolicy::DISABLED_V1).unwrap();
        assert_eq!(encoded, r#"{"version":1,"state":"disabled"}"#);
        assert_eq!(
            HumanBaseMergePolicy::parse(&encoded).unwrap(),
            HumanBaseMergePolicy::DISABLED_V1
        );
        assert!(!HumanBaseMergePolicy::DISABLED_V1.is_enabled());
        assert_eq!(
            HumanBaseMergePolicy::parse(r#"{"version":2,"state":"disabled"}"#),
            Err(HumanBaseMergePolicyError::UnsupportedVersion(2))
        );
        assert_eq!(
            HumanBaseMergePolicy::parse(r#"{"version":1,"state":"unknown"}"#),
            Err(HumanBaseMergePolicyError::InvalidEncoding)
        );
        assert_eq!(
            HumanBaseMergePolicy::parse(r#"{"version":1,"state":"disabled","extra":true}"#),
            Err(HumanBaseMergePolicyError::InvalidEncoding)
        );
        assert_eq!(
            serde_json::to_value(HumanBaseMergePolicyReadState::from_stored(
                r#"{"version":2,"state":"enabled"}"#
            ))
            .unwrap(),
            serde_json::json!({
                "availability": "unavailable",
                "reason": {"reason": "unsupported_version", "version": 2}
            })
        );
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveProject {
    pub project_id: String,
    pub team_id: String,
}

pub use cairn_cell::PopulateConfig;

/// One project check: a single command run at one cadence. Selectivity is
/// expressed by a `{changedFiles}` or `{targets}` placeholder inside the
/// command — the placeholder *is* the selector, and a placeholder-less command
/// runs as-is (a full run).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CheckCommand {
    /// The command to run. A `{changedFiles}` or `{targets}` placeholder makes
    /// it selective; a placeholder-less command runs as-is.
    pub command: String,
    /// Deliberate whole-suite entry point used by full-tree evaluations
    /// (main-health and manual runs), and by a selector check whose selection
    /// cannot narrow. When omitted, Cairn may derive a full command only from a
    /// safely trailing selector placeholder in `command`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impact: Option<Vec<String>>,
    /// Dependency-graph node(s) whose transitive closure IS this check's input
    /// set — `rust:<crate>` or `ts:<package>`, one token or a list whose closures
    /// union. The engine derives the closure from the sealed tree's own
    /// manifests, so the input set follows the real graph instead of a
    /// hand-maintained glob list that silently drifts out of date.
    ///
    /// Mutually exclusive with `impact`: they are two different definitions of
    /// the same check's inputs, and a check declaring both reports a config
    /// error rather than running under one of them. See docs/checks.md.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<CheckScopeSelector>,
    #[serde(
        default = "default_check_policy",
        skip_serializing_if = "is_default_check_policy"
    )]
    pub policy: CheckPolicy,
    #[serde(
        default = "default_check_when",
        skip_serializing_if = "is_default_check_when"
    )]
    pub when: CheckWhen,
    /// Host resource admission class. Shared checks consume one global permit;
    /// exclusive checks consume the controller's full capacity.
    #[serde(
        default = "default_check_resource_class",
        skip_serializing_if = "is_default_check_resource_class"
    )]
    pub resource_class: CheckResourceClass,
    /// Maximum wall-clock SECONDS this check may run before it is killed at its
    /// budget and recorded AS a timeout (not an opaque failure). `None` uses the
    /// cadence default — 10 min for `write`, 30 min for `review`. Clamped to a
    /// 60-minute hard ceiling. See docs/checks.md.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u32>,
    /// Which machine this check must run on, by public name or platform. The
    /// names it accepts are the ones `cairn://executors` publishes.
    ///
    /// This key had an earlier life as a scalar naming a build slot, and that
    /// meaning was retired. A config still carrying the scalar reads as no
    /// selector and is dropped on the next rewrite, exactly as it was before
    /// this field existed — the alternative is that one stale line stops the
    /// whole project's check contract from parsing, which takes down every
    /// check rather than the one that named it.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_executor_selector"
    )]
    pub executor: Option<ExecutorSelector>,
    /// Environment variable names whose values can change this check's verdict.
    /// Values are hashed into environment identity and are never persisted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verdict_environment: Vec<String>,

    /// The platform families whose result for this check the project gates on,
    /// as lowercase OS names (`macos`, `linux`, `windows`). Absent means the
    /// runner's own platform, which is what a project's gate has always meant:
    /// checks ran on the machine holding the checkout, so that machine's
    /// platform is the one the green was ever observed on.
    ///
    /// This is not "where this check CAN run" — that is `executor` — it is
    /// "whose answer counts as this check's verdict". Placement may only choose
    /// a machine on one of these platforms, so a suite can never turn a PR lane
    /// red because idleness sent it to a platform the project does not gate on.
    /// A genuinely cross-platform lane opts in by naming its platforms here.
    ///
    /// This intersects with `executor`: a selector can narrow where work is
    /// capable of running, but it cannot make an untrusted platform's answer
    /// count as the verdict.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_verdict_platforms"
    )]
    pub verdict_platforms: Option<Vec<String>>,

    /// Whether this check REWRITES the tree as its job — a formatter, not a
    /// verifier. Declared fixers run FIRST inside the write cadence's shared
    /// slot, so every other check in that wave already validates the fixed tree
    /// and the fold needs no second wave. Discovering a fixer by noticing slot
    /// dirt cannot buy that: by the time the dirt exists, the rest of the wave
    /// has already run against the tree the fix replaced. See docs/checks.md.
    #[serde(default, skip_serializing_if = "is_not_check_fixer")]
    pub fixes: bool,

    /// Run this check's whole suite on the default branch after every merge,
    /// on each named platform, and post each result to a project thread. Absent
    /// means the check never runs on main by itself. Not part of the check's
    /// result identity: where a verdict is announced does not change it. See
    /// docs/checks.md, "Main cadence".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_cadence: Option<MainCadence>,

    /// What the check's command sees. `full` (the default) runs it in a cell at
    /// a checkout of the commit under test. `impact` runs it on the runner, with
    /// no cell and no placement, in a scratch directory holding exactly the
    /// files the check's `impact` globs select at that commit: the same entries
    /// its cache key hashes, so the check can see only what its verdict is keyed
    /// on. See docs/checks.md, "Checks that need no tree".
    #[serde(default, skip_serializing_if = "CheckTree::is_full")]
    pub tree: CheckTree,

    /// For a `when: landing` check: how many SECONDS a merge waits for a cell
    /// to be admitted before it refuses as "gate unavailable". The run itself
    /// is bounded by `timeout`, separately. Absent uses the landing default.
    /// Not part of the check's result identity: how long a merge was willing
    /// to wait for a machine does not change what the machine answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_seconds: Option<u32>,
}

/// A check's `tree:` declaration.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CheckTree {
    /// A full checkout in a cell.
    #[default]
    Full,
    /// Only the `impact`-selected files, on the runner.
    Impact,
}

impl CheckTree {
    pub fn is_full(&self) -> bool {
        *self == CheckTree::Full
    }

    pub fn as_str(self) -> &'static str {
        match self {
            CheckTree::Full => "full",
            CheckTree::Impact => "impact",
        }
    }
}

/// A check's `mainCadence:` declaration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MainCadence {
    /// Lowercase OS names, each one the check counts a verdict from
    /// (`verdictPlatforms`). One lane runs per platform.
    pub platforms: Vec<String>,
    /// Name of the project thread every cadence row is posted to.
    pub notify: String,
}

/// Read a check's `executor:` key, tolerating the retired scalar form.
fn deserialize_executor_selector<'de, D>(
    deserializer: D,
) -> Result<Option<ExecutorSelector>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Declared {
        Selector(ExecutorSelector),
        /// The retired build-slot scalar. Typed as a string so only the retired
        /// shape is tolerated here — a malformed selector object still reports
        /// as one rather than being silently swallowed — and the value itself is
        /// never read, because the whole point is to drop it on the next
        /// rewrite.
        Retired(#[allow(dead_code)] String),
    }
    Ok(match Option::<Declared>::deserialize(deserializer)? {
        Some(Declared::Selector(selector)) => Some(selector),
        Some(Declared::Retired(_)) | None => None,
    })
}

/// A check's `scope:` declaration — one dependency-graph node token, or a list
/// of them whose closures union into a single input set.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum CheckScopeSelector {
    One(String),
    Many(Vec<String>),
}

impl CheckScopeSelector {
    /// The declared tokens, in declaration order.
    pub fn tokens(&self) -> Vec<String> {
        match self {
            CheckScopeSelector::One(token) => vec![token.clone()],
            CheckScopeSelector::Many(tokens) => tokens.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CheckPolicy {
    Advisory,
    Gate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CheckResourceClass {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CheckWhen {
    Write,
    /// Turn-end cadence: runs the fuller suites at every turn-end. `idle` is
    /// accepted as a legacy alias for un-migrated project configs — it used to
    /// be a separate every-turn-end cadence that `review` (once PR-gated) now
    /// subsumes — so an old `when: idle` still parses to this cadence instead of
    /// silently disabling every check for that project.
    #[serde(alias = "idle")]
    Review,
    /// Landing cadence: runs only when a pull request lands on the project's
    /// default branch, against the MERGE RESULT (the target's live tip plus
    /// the branch's change), before the target moves. A red refuses the merge;
    /// a check that cannot get a cell within its `budgetSeconds` refuses it as
    /// unavailable rather than passing it. Never runs at commit or turn end.
    /// See docs/checks.md, "The landing cadence".
    Landing,
}

fn default_check_policy() -> CheckPolicy {
    CheckPolicy::Advisory
}

fn is_default_check_policy(policy: &CheckPolicy) -> bool {
    *policy == default_check_policy()
}

fn default_check_resource_class() -> CheckResourceClass {
    CheckResourceClass::Shared
}

fn is_default_check_resource_class(resource_class: &CheckResourceClass) -> bool {
    *resource_class == default_check_resource_class()
}

fn is_not_check_fixer(fixes: &bool) -> bool {
    !*fixes
}

fn default_check_when() -> CheckWhen {
    CheckWhen::Write
}

fn is_default_check_when(when: &CheckWhen) -> bool {
    *when == default_check_when()
}

impl CheckPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckPolicy::Advisory => "advisory",
            CheckPolicy::Gate => "gate",
        }
    }
}

impl CheckResourceClass {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckResourceClass::Shared => "shared",
            CheckResourceClass::Exclusive => "exclusive",
        }
    }
}

impl CheckWhen {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckWhen::Write => "write",
            CheckWhen::Review => "review",
            CheckWhen::Landing => "landing",
        }
    }
}

/// A declared check this build declines to run, because one of its fields
/// carries a value outside the vocabulary the build was compiled with, or the
/// entry carries a key the build does not know.
///
/// `.cairn/config.yaml` lives in the repository, so it can be newer than the
/// running app: a merge can add a cadence before the app that reads it is
/// rebuilt. That config is not wrong, and the build cannot correct it, so the
/// one check that uses the value is set aside with this record and everything
/// else in the file loads as usual. This is deliberately unlike the rule for a
/// caller's own input, which is refused because the caller can fix it now.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UnsupportedCheck {
    /// The field holding the value, as written in the check entry. For a key
    /// this build does not know, the key itself.
    pub field: String,
    /// The value as written. A structured value is rendered inline.
    pub value: String,
    /// The values this build understands for `field`; empty when the field is
    /// structured rather than a closed list of words. For an unknown key, the
    /// keys a check entry may carry.
    pub known: Vec<String>,
    /// The sentence every surface shows for this check.
    pub notice: String,
    /// The entry exactly as declared, so a settings save by this build writes
    /// it back instead of deleting a check it could not read.
    #[serde(skip)]
    pub declared: serde_yaml::Value,
}

impl UnsupportedCheck {
    fn new(field: &str, value: String, known: Vec<String>, declared: serde_yaml::Value) -> Self {
        let mut notice =
            format!("needs a newer Cairn: `{field}: {value}` is not understood by this build");
        if !known.is_empty() {
            notice.push_str(&format!(" (it knows {})", known.join(", ")));
        }
        Self {
            field: field.to_string(),
            value,
            known,
            notice,
            declared,
        }
    }

    /// A key this build does not know. It may come from a newer Cairn, where it
    /// can change what the check means (a budget, a selector, a switch), or be
    /// a misspelling; this build cannot tell which, so it runs neither guess.
    fn unknown_key(
        key: &str,
        value: String,
        known: Vec<String>,
        declared: serde_yaml::Value,
    ) -> Self {
        Self {
            field: key.to_string(),
            value,
            known,
            notice: format!(
                "needs a newer Cairn, or the key is misspelled: `{key}` is not a check key this \
                 build understands"
            ),
            declared,
        }
    }
}

/// The keys a check entry may carry, and the values this build understands in
/// each closed-vocabulary field of one. [`CheckVocabulary::this_build`] reads
/// every list off the deserializer that parses it, so it cannot drift from what
/// the build parses; a test narrows it to stand in for an older build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckVocabulary {
    pub keys: Vec<String>,
    pub policy: Vec<String>,
    pub when: Vec<String>,
    pub resource_class: Vec<String>,
    pub tree: Vec<String>,
    pub verdict_platforms: Vec<String>,
}

impl CheckVocabulary {
    pub fn this_build() -> Self {
        Self {
            keys: serde_fields::<CheckCommand>(),
            policy: serde_variants::<CheckPolicy>(),
            when: serde_variants::<CheckWhen>(),
            resource_class: serde_variants::<CheckResourceClass>(),
            tree: serde_variants::<CheckTree>(),
            verdict_platforms: VERDICT_PLATFORMS.iter().map(|p| p.to_string()).collect(),
        }
    }

    /// Why this build cannot read `declared`, when it cannot: the first value
    /// this vocabulary does not cover, else the first key it does not know.
    fn set_aside(&self, declared: &serde_yaml::Value) -> Option<UnsupportedCheck> {
        let unsupported = |field: &str, value: String, known: Vec<String>| {
            Some(UnsupportedCheck::new(field, value, known, declared.clone()))
        };
        let entry = declared.as_mapping()?;
        for (field, known) in [
            ("policy", &self.policy),
            ("when", &self.when),
            ("resourceClass", &self.resource_class),
            ("tree", &self.tree),
        ] {
            if let Some(serde_yaml::Value::String(value)) = entry.get(field) {
                if !known.contains(value) {
                    return unsupported(field, value.clone(), known.clone());
                }
            }
        }
        if let Some(serde_yaml::Value::Sequence(platforms)) = entry.get("verdictPlatforms") {
            for platform in platforms {
                let serde_yaml::Value::String(value) = platform else {
                    continue;
                };
                let normalized = value.trim().to_ascii_lowercase();
                if !normalized.is_empty() && !self.verdict_platforms.contains(&normalized) {
                    return unsupported(
                        "verdictPlatforms",
                        value.clone(),
                        self.verdict_platforms.clone(),
                    );
                }
            }
        }
        // The selector nests closed vocabularies of its own (the containment
        // envelope's process classes and edges), so it is judged whole by the
        // deserializer the check itself uses.
        if let Some(executor) = entry.get("executor") {
            if deserialize_executor_selector(executor.clone()).is_err() {
                return unsupported("executor", render_inline(executor), Vec::new());
            }
        }
        // A key the build does not know is judged here rather than ignored by
        // the strict parse: ignoring it would run the check under a meaning its
        // author did not write. A non-string key is left to the strict parse,
        // which reports it as malformed.
        entry.iter().find_map(|(key, value)| {
            let key = key.as_str()?;
            (!self.keys.iter().any(|known| known == key)).then(|| {
                UnsupportedCheck::unknown_key(
                    key,
                    render_inline(value),
                    self.keys.clone(),
                    declared.clone(),
                )
            })
        })
    }
}

/// One declared check entry, read under a vocabulary.
#[derive(Debug, Clone, PartialEq)]
pub enum DeclaredCheck {
    Supported(CheckCommand),
    Unsupported(UnsupportedCheck),
}

/// Read one check entry. A value outside `vocabulary` sets the check aside as
/// [`DeclaredCheck::Unsupported`], as does a key the vocabulary does not list;
/// anything else wrong with the entry, such as a missing `command`, stays an
/// error.
pub fn read_declared_check(
    declared: serde_yaml::Value,
    vocabulary: &CheckVocabulary,
) -> Result<DeclaredCheck, serde_yaml::Error> {
    if let Some(unsupported) = vocabulary.set_aside(&declared) {
        return Ok(DeclaredCheck::Unsupported(unsupported));
    }
    // Deserialized from the entry's text, not from the `Value`: in a `Value` a
    // plain `false` or `1800` is already a bool or a number, which a `String`
    // field refuses, while the text deserializer reads a plain scalar as a
    // string wherever the field wants one. `command: false` must keep meaning
    // the shell command it meant when the whole file was parsed from text.
    let text = serde_yaml::to_string(&declared)?;
    serde_yaml::from_str(&text).map(DeclaredCheck::Supported)
}

fn render_inline(value: &serde_yaml::Value) -> String {
    match value {
        serde_yaml::Value::String(value) => value.clone(),
        other => serde_json::to_string(other).unwrap_or_else(|_| {
            serde_yaml::to_string(other)
                .unwrap_or_default()
                .trim()
                .to_string()
        }),
    }
}

/// The names a derived serde enum accepts, aliases included, captured from
/// the variant list its `Deserialize` impl hands the deserializer.
fn serde_variants<T: serde::de::DeserializeOwned>() -> Vec<String> {
    serde_names::<T>("a check vocabulary field is a derived serde enum")
}

/// The keys a derived serde struct accepts, captured from the field list its
/// `Deserialize` impl hands the deserializer.
fn serde_fields<T: serde::de::DeserializeOwned>() -> Vec<String> {
    serde_names::<T>("a check entry is a derived serde struct")
}

/// The name list a derived `Deserialize` impl declares up front: an enum's
/// variants or a struct's fields. Nothing is deserialized.
fn serde_names<T: serde::de::DeserializeOwned>(expectation: &str) -> Vec<String> {
    use serde::de::{Error, Visitor};

    struct Probe<'a>(&'a mut Option<&'static [&'static str]>);

    impl<'de> serde::Deserializer<'de> for Probe<'_> {
        type Error = serde::de::value::Error;

        fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
            Err(Self::Error::custom("not a derived enum or struct"))
        }

        fn deserialize_enum<V: Visitor<'de>>(
            self,
            _name: &'static str,
            variants: &'static [&'static str],
            _visitor: V,
        ) -> Result<V::Value, Self::Error> {
            *self.0 = Some(variants);
            Err(Self::Error::custom("variants captured"))
        }

        fn deserialize_struct<V: Visitor<'de>>(
            self,
            _name: &'static str,
            fields: &'static [&'static str],
            _visitor: V,
        ) -> Result<V::Value, Self::Error> {
            *self.0 = Some(fields);
            Err(Self::Error::custom("fields captured"))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
            bytes byte_buf option unit unit_struct newtype_struct seq tuple
            tuple_struct map identifier ignored_any
        }
    }

    let mut names = None;
    let _ = T::deserialize(Probe(&mut names));
    names
        .expect(expectation)
        .iter()
        .map(|name| name.to_string())
        .collect()
}

#[cfg(test)]
mod check_vocabulary_tests {
    use super::*;

    fn entry(yaml: &str) -> serde_yaml::Value {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn this_build_vocabulary_is_what_the_enums_accept() {
        let vocabulary = CheckVocabulary::this_build();
        assert_eq!(
            vocabulary.keys,
            [
                "command",
                "fullCommand",
                "impact",
                "scope",
                "policy",
                "when",
                "resourceClass",
                "timeout",
                "executor",
                "verdictEnvironment",
                "verdictPlatforms",
                "fixes",
                "mainCadence",
                "tree",
                "budgetSeconds",
            ]
        );
        assert_eq!(vocabulary.policy, ["advisory", "gate"]);
        // `idle` is the legacy alias of `review`; the vocabulary must accept
        // exactly what the deserializer does, alias included.
        assert_eq!(vocabulary.when, ["write", "idle", "review", "landing"]);
        assert_eq!(vocabulary.resource_class, ["shared", "exclusive"]);
        assert_eq!(vocabulary.tree, ["full", "impact"]);
        assert_eq!(vocabulary.verdict_platforms, ["macos", "linux", "windows"]);
    }

    #[test]
    fn every_known_word_parses_under_the_real_enum() {
        // The vocabulary is consulted BEFORE the strict parse, so a word it
        // lists that the enum rejected would turn an unsupported check into a
        // malformed one.
        let vocabulary = CheckVocabulary::this_build();
        for (field, words) in [
            ("policy", &vocabulary.policy),
            ("when", &vocabulary.when),
            ("resourceClass", &vocabulary.resource_class),
            ("tree", &vocabulary.tree),
        ] {
            for word in words {
                let declared = entry(&format!("command: x\n{field}: {word}\n"));
                assert!(
                    matches!(
                        read_declared_check(declared, &vocabulary),
                        Ok(DeclaredCheck::Supported(_))
                    ),
                    "{field}: {word}"
                );
            }
        }
    }

    #[test]
    fn a_word_an_older_build_lacks_sets_the_check_aside_with_its_value() {
        let mut older = CheckVocabulary::this_build();
        older.when.retain(|word| word != "landing");
        let declared = entry("command: cargo check\nwhen: landing\nbudgetSeconds: 600\n");
        let DeclaredCheck::Unsupported(unsupported) =
            read_declared_check(declared.clone(), &older).unwrap()
        else {
            panic!("`when: landing` must be unsupported under a build without it");
        };
        assert_eq!(unsupported.field, "when");
        assert_eq!(unsupported.value, "landing");
        assert_eq!(unsupported.known, ["write", "idle", "review"]);
        assert_eq!(
            unsupported.notice,
            "needs a newer Cairn: `when: landing` is not understood by this build \
             (it knows write, idle, review)"
        );
        assert_eq!(unsupported.declared, declared);
    }

    #[test]
    fn every_closed_field_is_covered() {
        let vocabulary = CheckVocabulary::this_build();
        for (yaml, field, value) in [
            ("command: x\npolicy: blocking\n", "policy", "blocking"),
            ("command: x\nwhen: nightly\n", "when", "nightly"),
            ("command: x\nresourceClass: gpu\n", "resourceClass", "gpu"),
            ("command: x\ntree: sparse\n", "tree", "sparse"),
            (
                "command: x\nverdictPlatforms: [linux, freebsd]\n",
                "verdictPlatforms",
                "freebsd",
            ),
        ] {
            let DeclaredCheck::Unsupported(unsupported) =
                read_declared_check(entry(yaml), &vocabulary).unwrap()
            else {
                panic!("{yaml} must be unsupported");
            };
            assert_eq!(
                (unsupported.field.as_str(), unsupported.value.as_str()),
                (field, value)
            );
        }

        let DeclaredCheck::Unsupported(executor) = read_declared_check(
            entry(
                "command: x\nexecutor:\n  containment:\n    processClass: quantum\n    edges: []\n",
            ),
            &vocabulary,
        )
        .unwrap() else {
            panic!("a selector this build cannot read must be unsupported");
        };
        assert_eq!(executor.field, "executor");
        assert!(executor.value.contains("quantum"), "{}", executor.value);
        assert!(executor.known.is_empty());
        assert!(!executor.notice.contains("it knows"), "{}", executor.notice);
    }

    #[test]
    fn a_plain_scalar_reads_as_a_string_where_the_field_wants_one() {
        let vocabulary = CheckVocabulary::this_build();
        for (yaml, command) in [
            ("command: false\n", "false"),
            ("command: true\n", "true"),
            ("command: 7\n", "7"),
            ("command: 'false'\n", "false"),
        ] {
            let Ok(DeclaredCheck::Supported(check)) = read_declared_check(entry(yaml), &vocabulary)
            else {
                panic!("{yaml} must read as a supported check");
            };
            assert_eq!(check.command, command, "{yaml}");
        }
    }

    #[test]
    fn a_malformed_entry_is_still_an_error() {
        let vocabulary = CheckVocabulary::this_build();
        assert!(read_declared_check(entry("when: review\n"), &vocabulary).is_err());
        // A non-word where a word belongs is malformed, not a newer word.
        assert!(read_declared_check(entry("command: x\nwhen: 3\n"), &vocabulary).is_err());
        // The retired build-slot scalar is still tolerated, not unsupported.
        assert!(matches!(
            read_declared_check(entry("command: x\nexecutor: build-slot\n"), &vocabulary),
            Ok(DeclaredCheck::Supported(_))
        ));
    }

    #[test]
    fn a_key_this_build_does_not_know_sets_the_check_aside() {
        // `budgetSeconds` before the build that introduced it: the check would
        // otherwise run with no budget, a meaning its author did not write.
        let mut older = CheckVocabulary::this_build();
        older.keys.retain(|key| key != "budgetSeconds");
        let declared = entry("command: cargo check\nbudgetSeconds: 600\n");
        let DeclaredCheck::Unsupported(unsupported) =
            read_declared_check(declared.clone(), &older).unwrap()
        else {
            panic!("a key the build lacks must set the check aside");
        };
        assert_eq!(
            (unsupported.field.as_str(), unsupported.value.as_str()),
            ("budgetSeconds", "600")
        );
        assert_eq!(
            unsupported.notice,
            "needs a newer Cairn, or the key is misspelled: `budgetSeconds` is not a check key \
             this build understands"
        );
        assert_eq!(unsupported.declared, declared);
        // The same entry is fully readable by the build that knows the key.
        assert!(matches!(
            read_declared_check(declared, &CheckVocabulary::this_build()),
            Ok(DeclaredCheck::Supported(_))
        ));

        // A misspelling reads the same way: the build cannot tell it from a
        // newer key, and says both.
        let DeclaredCheck::Unsupported(typo) = read_declared_check(
            entry("command: x\ntimout: 60\n"),
            &CheckVocabulary::this_build(),
        )
        .unwrap() else {
            panic!("a misspelled key must not be silently ignored");
        };
        assert_eq!(typo.field, "timout");
        assert!(typo.notice.contains("misspelled"), "{}", typo.notice);
    }
}
