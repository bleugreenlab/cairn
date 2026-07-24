//! Resource-contract type definitions.
//!
//! The data-free enums and the spec structs the `RESOURCE_CONTRACTS` table is
//! built from. The table itself and the reusable key specs live in sibling
//! modules.

/// Supported operations for the canonical `write` carrier.
///
/// Lives here (not in `cairn-core`) so the contract table and the core
/// dispatcher share one definition. `cairn-core` re-exports it.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeMode {
    Create,
    Append,
    Patch,
    #[serde(rename = "unified_patch")]
    UnifiedPatch,
    Replace,
    Delete,
    Rename,
    Apply,
}

impl ChangeMode {
    /// Every mode, for exhaustive enumeration (parity test).
    pub const ALL: &'static [ChangeMode] = &[
        ChangeMode::Create,
        ChangeMode::Append,
        ChangeMode::Patch,
        ChangeMode::UnifiedPatch,
        ChangeMode::Replace,
        ChangeMode::Delete,
        ChangeMode::Rename,
        ChangeMode::Apply,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ChangeMode::Create => "create",
            ChangeMode::Append => "append",
            ChangeMode::Patch => "patch",
            ChangeMode::UnifiedPatch => "unified_patch",
            ChangeMode::Replace => "replace",
            ChangeMode::Delete => "delete",
            ChangeMode::Rename => "rename",
            ChangeMode::Apply => "apply",
        }
    }
}

/// Data-free mirror of `CairnResource` used to key the table and to enumerate
/// resources for the parity test. `CairnResource::kind()` maps each variant
/// here 1:1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    Project,
    ProjectIssues,
    ProjectMessages,
    ProjectTerminal,
    ProjectBrowser,
    ProjectBrowserNetworkRequest,
    Issue,
    Changed,
    IssueExecutions,
    IssueExecution,
    IssueMessages,
    IssueComments,
    IssueComment,
    Node,
    NodeChat,
    NodeChatRaw,
    NodeChatTurn,
    NodeChatEvent,
    NodeArtifact,
    NodeDiff,
    NodeTerminal,
    NodeRepl,
    NodeBrowser,
    NodeBrowserNetworkRequest,
    NodeMessages,
    NodeProgress,
    TaskTerminal,
    TaskBrowser,
    TaskBrowserNetworkRequest,
    Task,
    TaskChat,
    TaskChatRaw,
    TaskChatTurn,
    TaskChatEvent,
    TaskArtifact,
    TaskMessages,
    JobTodos,
    NodeTasks,
    NodeCalls,
    NodeWakes,
    NodeChecks,
    TaskChecks,
    NodeQuestions,
    NodeQuestion,
    NodePermissions,
    NodePermission,
    TaskPermissions,
    TaskPermission,
    Db,
    Dev,
    DevDb,
    DevPid,
    Logs,
    Bug,
    Skills,
    Skill,
    ProjectSkills,
    ProjectSkill,
    ProjectReferences,
    ProjectReference,
    Labels,
    Label,
    NodeMemories,
    NodeMemory,
    Recipes,
    Recipe,
    ProjectRecipes,
    ProjectRecipe,
    Workflows,
    Workflow,
    ProjectWorkflows,
    ProjectWorkflow,
    Agents,
    Agent,
    ProjectAgents,
    ProjectAgent,
    Actions,
    Action,
    ProjectActions,
    ProjectAction,
    NodeSymbols,
    ProjectSymbols,
    Help,
    WebSearch,
    Mcp,
    Settings,
    Projects,
    ProjectSettings,
}

impl ResourceKind {
    /// Every kind, for exhaustive enumeration (parity test + advertising).
    pub(crate) const ALL: &'static [ResourceKind] = &[
        ResourceKind::Project,
        ResourceKind::ProjectIssues,
        ResourceKind::ProjectMessages,
        ResourceKind::ProjectTerminal,
        ResourceKind::ProjectBrowser,
        ResourceKind::ProjectBrowserNetworkRequest,
        ResourceKind::Issue,
        ResourceKind::Changed,
        ResourceKind::IssueExecutions,
        ResourceKind::IssueExecution,
        ResourceKind::IssueMessages,
        ResourceKind::IssueComments,
        ResourceKind::IssueComment,
        ResourceKind::Node,
        ResourceKind::NodeChat,
        ResourceKind::NodeChatRaw,
        ResourceKind::NodeChatTurn,
        ResourceKind::NodeChatEvent,
        ResourceKind::NodeArtifact,
        ResourceKind::NodeDiff,
        ResourceKind::NodeTerminal,
        ResourceKind::NodeRepl,
        ResourceKind::NodeBrowser,
        ResourceKind::NodeBrowserNetworkRequest,
        ResourceKind::NodeMessages,
        ResourceKind::NodeProgress,
        ResourceKind::TaskTerminal,
        ResourceKind::TaskBrowser,
        ResourceKind::TaskBrowserNetworkRequest,
        ResourceKind::Task,
        ResourceKind::TaskChat,
        ResourceKind::TaskChatRaw,
        ResourceKind::TaskChatTurn,
        ResourceKind::TaskChatEvent,
        ResourceKind::TaskArtifact,
        ResourceKind::TaskMessages,
        ResourceKind::JobTodos,
        ResourceKind::NodeTasks,
        ResourceKind::NodeCalls,
        ResourceKind::NodeWakes,
        ResourceKind::NodeChecks,
        ResourceKind::TaskChecks,
        ResourceKind::NodeQuestions,
        ResourceKind::NodeQuestion,
        ResourceKind::NodePermissions,
        ResourceKind::NodePermission,
        ResourceKind::TaskPermissions,
        ResourceKind::TaskPermission,
        ResourceKind::Db,
        ResourceKind::Dev,
        ResourceKind::DevDb,
        ResourceKind::DevPid,
        ResourceKind::Logs,
        ResourceKind::Bug,
        ResourceKind::Skills,
        ResourceKind::Skill,
        ResourceKind::ProjectSkills,
        ResourceKind::ProjectSkill,
        ResourceKind::ProjectReferences,
        ResourceKind::ProjectReference,
        ResourceKind::Labels,
        ResourceKind::Label,
        ResourceKind::NodeMemories,
        ResourceKind::NodeMemory,
        ResourceKind::Recipes,
        ResourceKind::Recipe,
        ResourceKind::ProjectRecipes,
        ResourceKind::ProjectRecipe,
        ResourceKind::Workflows,
        ResourceKind::Workflow,
        ResourceKind::ProjectWorkflows,
        ResourceKind::ProjectWorkflow,
        ResourceKind::Agents,
        ResourceKind::Agent,
        ResourceKind::ProjectAgents,
        ResourceKind::ProjectAgent,
        ResourceKind::Actions,
        ResourceKind::Action,
        ResourceKind::ProjectActions,
        ResourceKind::ProjectAction,
        ResourceKind::NodeSymbols,
        ResourceKind::ProjectSymbols,
        ResourceKind::Help,
        ResourceKind::WebSearch,
        ResourceKind::Mcp,
        ResourceKind::Settings,
        ResourceKind::Projects,
        ResourceKind::ProjectSettings,
    ];

    /// Kebab-lowercased variant name (`Issue` -> `issue`, `NodeArtifact` ->
    /// `node-artifact`). Single-sourced from the `Debug` variant name so it can
    /// never drift from the enum; used as the `cairn://help?kind=<slug>`
    /// selector that the session-scoped affordance pointer targets.
    pub fn slug(self) -> String {
        let name = format!("{self:?}");
        let mut out = String::with_capacity(name.len() + 4);
        for (i, ch) in name.chars().enumerate() {
            if ch.is_ascii_uppercase() {
                if i != 0 {
                    out.push('-');
                }
                out.push(ch.to_ascii_lowercase());
            } else {
                out.push(ch);
            }
        }
        out
    }

    /// Resolve a [`Self::slug`] back to its kind. `None` for an unknown slug.
    pub fn from_slug(slug: &str) -> Option<ResourceKind> {
        ResourceKind::ALL
            .iter()
            .copied()
            .find(|kind| kind.slug() == slug)
    }
}

/// Rough JSON type of a payload key, for documentation in rejections/affordances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Str,
    Bool,
    Int,
    Array,
    Object,
}

impl KeyType {
    pub fn as_str(self) -> &'static str {
        match self {
            KeyType::Str => "str",
            KeyType::Bool => "bool",
            KeyType::Int => "int",
            KeyType::Array => "array",
            KeyType::Object => "object",
        }
    }
}

/// One payload key a mutation reads.
#[derive(Debug, Clone, Copy)]
pub struct KeySpec {
    /// Canonical key name (advertised in examples).
    pub key: &'static str,
    /// Accepted alternate spellings (e.g. snake_case for a camelCase key).
    ///
    /// Every alias advertised here must be honored by the downstream handler or
    /// deserializer, not just by the gate: `satisfied_by` lets an alias clear the
    /// required-key gate, but a handler that only reads the canonical spelling
    /// would then silently drop the field. The dispatch-level
    /// `advertised_aliases_are_honored_by_dispatch` test pins the gate-observable
    /// (required-key) cases; struct-deserialized optional aliases are pinned
    /// individually (see `agent_frontmatter_honors_model_alias_for_tier`).
    pub aliases: &'static [&'static str],
    pub ty: KeyType,
    note: &'static str,
}

impl KeySpec {
    pub(crate) const fn new(key: &'static str, ty: KeyType, note: &'static str) -> Self {
        Self {
            key,
            aliases: &[],
            ty,
            note,
        }
    }

    pub(crate) const fn with_aliases(
        key: &'static str,
        aliases: &'static [&'static str],
        ty: KeyType,
        note: &'static str,
    ) -> Self {
        Self {
            key,
            aliases,
            ty,
            note,
        }
    }

    /// True when `key` or any alias is present among the supplied payload keys.
    pub fn satisfied_by<'a>(&self, mut keys: impl Iterator<Item = &'a str>) -> bool {
        keys.any(|present| present == self.key || self.aliases.contains(&present))
    }

    /// `key(type)` for self-evident keys, `key(type, note)` when the note carries
    /// value guidance (enumerations, defaults). Shared by the affordance blocks
    /// and the system-prompt mutation reference so they never diverge.
    pub fn display(&self) -> String {
        if self.note.is_empty() {
            format!("{}({})", self.key, self.ty.as_str())
        } else {
            format!("{}({}, {})", self.key, self.ty.as_str(), self.note)
        }
    }
}

/// One supported mutation for a resource.
///
/// Editing a `label` or `example` here also changes the affordance blocks
/// rendered on read, which the `affordance_for_*_kind_*` assertion tests in
/// `cairn-core/src/resources/common.rs` consume verbatim. Those tests surface
/// only at a full `bun run test:rust`, never at `bun run check:rust`, so update
/// them in the same change.
#[derive(Debug, Clone, Copy)]
pub struct MutationSpec {
    pub mode: ChangeMode,
    pub required: &'static [KeySpec],
    pub optional: &'static [KeySpec],
    /// Short human label ("create issue", "start terminal").
    pub label: &'static str,
    /// One-line, ready-to-copy `write` call.
    pub example: &'static str,
}

/// One read-side query projection (`?key=values`).
#[derive(Debug, Clone, Copy)]
pub struct ProjectionSpec {
    pub key: &'static str,
    pub values: &'static str,
}

/// A related resource surfaced from a resource's affordance block.
#[derive(Debug, Clone, Copy)]
pub struct RelatedSpec {
    pub label: &'static str,
    pub kind: ResourceKind,
    pub actions: bool,
}

/// A mutation that lives on ANOTHER resource but takes THIS resource as input.
///
/// Some workflows can't be expressed by a resource's own mutation set: a recipe
/// is the `{recipe}` input to starting an execution, but that `append` mutation
/// lives on the executions resource, not the recipe. A cross-action references
/// the owning `(kind, mode)` so the affordance can render "this resource feeds
/// that mutation over there" using the target's `uri_template` and example —
/// restoring a deliberate hint the contract-derived affordances otherwise drop.
#[derive(Debug, Clone, Copy)]
pub struct CrossActionSpec {
    /// The resource kind that owns the mutation.
    pub kind: ResourceKind,
    /// Which mutation on that kind this resource feeds.
    pub mode: ChangeMode,
    /// Action label framed from this resource's perspective.
    pub label: &'static str,
}

/// The full contract for one resource kind.
#[derive(Debug, Clone, Copy)]
pub struct ResourceContract {
    pub kind: ResourceKind,
    pub uri_template: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub read_projections: &'static [ProjectionSpec],
    pub related: &'static [RelatedSpec],
    /// Mutations on OTHER resources that take this resource as input. Rendered in
    /// this kind's actions section from the target's `uri_template` + example.
    pub cross_actions: &'static [CrossActionSpec],
    pub mutations: &'static [MutationSpec],
}

impl ResourceContract {
    /// Find the spec for a mode, if this resource supports it.
    pub fn mutation(&self, mode: ChangeMode) -> Option<&'static MutationSpec> {
        self.mutations.iter().find(|spec| spec.mode == mode)
    }
}

#[cfg(test)]
mod kind_slug_tests {
    use super::ResourceKind;

    #[test]
    fn slug_is_kebab_lowercase_and_roundtrips() {
        assert_eq!(ResourceKind::Issue.slug(), "issue");
        assert_eq!(ResourceKind::NodeArtifact.slug(), "node-artifact");
        assert_eq!(ResourceKind::ProjectIssues.slug(), "project-issues");
        // Every kind's slug resolves back to exactly that kind, so slugs are a
        // total, injective naming of the enum the help projection can rely on.
        for kind in ResourceKind::ALL {
            assert_eq!(ResourceKind::from_slug(&kind.slug()), Some(*kind));
        }
    }

    #[test]
    fn from_slug_rejects_unknown() {
        assert_eq!(ResourceKind::from_slug("not-a-kind"), None);
    }
}
