//! Recipe types - workflow definitions.

use serde::{Deserialize, Deserializer, Serialize};

/// Recipe trigger - when a recipe can be started
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecipeTrigger {
    #[default]
    #[serde(alias = "issue")]
    Manual,
    Schedule,
    JobEnded,
    SkillCalled,
}

impl std::fmt::Display for RecipeTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecipeTrigger::Manual => write!(f, "manual"),
            RecipeTrigger::Schedule => write!(f, "schedule"),
            RecipeTrigger::JobEnded => write!(f, "job_ended"),
            RecipeTrigger::SkillCalled => write!(f, "skill_called"),
        }
    }
}

impl std::str::FromStr for RecipeTrigger {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "manual" | "issue" => Ok(RecipeTrigger::Manual),
            "schedule" => Ok(RecipeTrigger::Schedule),
            "job_ended" => Ok(RecipeTrigger::JobEnded),
            "skill_called" => Ok(RecipeTrigger::SkillCalled),
            _ => Err(format!("Unknown recipe trigger: {}", s)),
        }
    }
}

/// Trigger scope - where the recipe executes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TriggerScope {
    #[default]
    Issue,
    Project,
}

impl std::fmt::Display for TriggerScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerScope::Issue => write!(f, "issue"),
            TriggerScope::Project => write!(f, "project"),
        }
    }
}

impl std::str::FromStr for TriggerScope {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "issue" => Ok(TriggerScope::Issue),
            "project" => Ok(TriggerScope::Project),
            _ => Err(format!("Unknown trigger scope: {}", s)),
        }
    }
}

/// Agent filter mode — allow or exclude listed agents.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AgentFilterMode {
    Allow,
    Exclude,
}

/// Structured agent filter with allow/exclude semantics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentFilter {
    pub mode: AgentFilterMode,
    pub ids: Vec<String>,
}

/// Custom deserializer for node_filter that accepts both the old String format
/// and the new AgentFilter struct for backward compatibility.
fn deserialize_node_filter<'de, D>(deserializer: D) -> Result<Option<AgentFilter>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NodeFilterValue {
        Struct(AgentFilter),
        String(String),
    }

    let opt: Option<NodeFilterValue> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(NodeFilterValue::Struct(filter)) => Ok(Some(filter)),
        Some(NodeFilterValue::String(s)) => {
            if s.is_empty() {
                Ok(None)
            } else {
                Ok(Some(AgentFilter {
                    mode: AgentFilterMode::Allow,
                    ids: vec![s],
                }))
            }
        }
    }
}

/// Serialize AgentFilter — skip if None.
fn serialize_node_filter<S>(value: &Option<AgentFilter>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(filter) => filter.serialize(serializer),
        None => serializer.serialize_none(),
    }
}

/// Event filter for event-driven triggers (JobEnded, SkillCalled)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventFilter {
    /// For JobEnded: filter by job status (e.g., ["complete", "failed"])
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_status: Option<Vec<String>>,
    /// For SkillCalled: filter by skill IDs (e.g., ["code-review"])
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_ids: Option<Vec<String>>,
    /// Filter by agent config ID (allow/exclude list)
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_node_filter",
        serialize_with = "serialize_node_filter"
    )]
    pub node_filter: Option<AgentFilter>,

    // --- Accumulation ---
    /// Fire after every N matching events (default/absent/1 = immediate)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub every: Option<i32>,
    /// Group events by this field in the event payload (required when every > 1)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_by: Option<String>,
    /// Scope override for accumulation: "global", "project", "issue"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accumulation_scope: Option<AccumulationScope>,
    /// Only count events within this window (seconds). Older events are pruned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_window_secs: Option<i64>,
}

/// Accumulation scope — how events are grouped for counting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AccumulationScope {
    Global,
    #[default]
    Project,
    Issue,
}

/// Recipe - a workflow definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Recipe {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub trigger: RecipeTrigger,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    /// System recipes are backend/machinery workflows hidden from the
    /// issue-create picker; they stay listed and runnable in Settings.
    #[serde(default)]
    pub is_system: bool,
    pub version: i32,
    pub parent_recipe_id: Option<String>,
    pub child_recipe_id: Option<String>,
    pub nodes: Vec<RecipeNode>,
    pub edges: Vec<RecipeEdge>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Recipe {
    /// Get the trigger scope from the trigger node's config.
    /// Defaults to Issue if no trigger node or no scope specified.
    pub fn scope(&self) -> TriggerScope {
        self.nodes
            .iter()
            .find(|n| n.node_type == RecipeNodeType::Trigger)
            .and_then(|n| n.trigger_config.as_ref())
            .and_then(|tc| tc.scope.clone())
            .unwrap_or(TriggerScope::Issue)
    }
}

/// Input for creating a recipe
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRecipe {
    pub name: String,
    pub description: Option<String>,
    pub trigger: Option<RecipeTrigger>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    /// Marks the recipe as a system workflow (hidden from the issue-create picker).
    pub is_system: Option<bool>,
    pub nodes: Option<Vec<RecipeNode>>,
    pub edges: Option<Vec<RecipeEdge>>,
}

/// Input for updating a recipe
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateRecipe {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub trigger: Option<RecipeTrigger>,
    #[allow(dead_code)]
    pub workspace_id: Option<Option<String>>,
    #[allow(dead_code)]
    pub project_id: Option<Option<String>>,
    /// When present, sets whether the recipe is a system workflow.
    pub is_system: Option<bool>,
    pub nodes: Option<Vec<RecipeNode>>,
    pub edges: Option<Vec<RecipeEdge>>,
}

// ============================================================================
// Recipe Node types (DAG-based structure)
// ============================================================================

/// Recipe node type enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RecipeNodeType {
    Trigger,
    Agent,
    Action,
    Pr,
    Checkpoint,
    Artifact,
    Condition,
    Context,
}

impl std::fmt::Display for RecipeNodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecipeNodeType::Trigger => write!(f, "trigger"),
            RecipeNodeType::Agent => write!(f, "agent"),
            RecipeNodeType::Action => write!(f, "action"),
            RecipeNodeType::Pr => write!(f, "pr"),
            RecipeNodeType::Checkpoint => write!(f, "checkpoint"),
            RecipeNodeType::Artifact => write!(f, "artifact"),
            RecipeNodeType::Condition => write!(f, "condition"),
            RecipeNodeType::Context => write!(f, "context"),
        }
    }
}

impl std::str::FromStr for RecipeNodeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "trigger" => Ok(RecipeNodeType::Trigger),
            "agent" => Ok(RecipeNodeType::Agent),
            "action" => Ok(RecipeNodeType::Action),
            "pr" => Ok(RecipeNodeType::Pr),
            "checkpoint" => Ok(RecipeNodeType::Checkpoint),
            "artifact" => Ok(RecipeNodeType::Artifact),
            "condition" => Ok(RecipeNodeType::Condition),
            "context" => Ok(RecipeNodeType::Context),
            _ => Err(format!("Unknown recipe node type: {}", s)),
        }
    }
}

/// Recipe edge type enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum RecipeEdgeType {
    Control,
    Context,
}

impl std::fmt::Display for RecipeEdgeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecipeEdgeType::Control => write!(f, "control"),
            RecipeEdgeType::Context => write!(f, "context"),
        }
    }
}

impl std::str::FromStr for RecipeEdgeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "control" => Ok(RecipeEdgeType::Control),
            "context" => Ok(RecipeEdgeType::Context),
            _ => Err(format!("Unknown recipe edge type: {}", s)),
        }
    }
}

/// Node position in the DAG editor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePosition {
    pub x: f32,
    pub y: f32,
}

/// Type-specific configuration stored as JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeConfig {
    // Trigger config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_type: Option<RecipeTrigger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<TriggerScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_config: Option<ScheduleConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_filter: Option<EventFilter>,

    // Agent config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_config_id: Option<String>,
    /// Nested output schema for agent nodes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<SchemaConfig>,
    /// Git/worktree configuration for agent nodes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_config: Option<AgentGitConfig>,

    // Action config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_config_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_params: Option<serde_json::Value>,
    /// Nested input schema for action nodes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<SchemaConfig>,
    /// Nested output schema for action nodes (uses output_schema field above)

    // Standalone checkpoint node config (command gate). A `checkpoint`-type node
    // runs this command: exit 0 passes (continue), non-zero blocks (resumable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,

    // Legacy artifact config (for standalone artifact nodes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,

    // Condition config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_type: Option<ConditionType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_error: Option<ConditionErrorBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_port: Option<String>,

    // Context config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// Recipe node - a single node in the workflow DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeNode {
    pub id: String,
    pub node_type: RecipeNodeType,
    pub name: String,
    pub position: NodePosition,
    /// For slot nodes (checkpoint, artifact): references the parent node they attach to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_config: Option<TriggerConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_config: Option<AgentNodeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_config: Option<ActionNodeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_config: Option<CheckpointNodeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_config: Option<ArtifactNodeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_config: Option<ConditionNodeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_config: Option<ContextNodeConfig>,
}

/// Schedule period - predefined intervals
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchedulePeriod {
    Month,
    Week,
    Day,
    Hour,
}

/// Schedule interval - custom duration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleInterval {
    pub days: u32,
    pub hours: u32,
    pub minutes: u32,
}

/// Schedule every - period or custom interval
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ScheduleEvery {
    Period(SchedulePeriod),
    Interval(ScheduleInterval),
}

/// Schedule at - specific time within a period
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleAt {
    /// Day: 1-31 for month, 0-6 for week (Sunday=0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day: Option<u32>,
    pub hour: u32,
    pub minute: u32,
}

/// Schedule configuration for scheduled recipes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleConfig {
    pub every: ScheduleEvery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<ScheduleAt>,
    pub allow_catchup: bool,
    pub catchup_window_hours: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerConfig {
    pub trigger_type: RecipeTrigger,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<TriggerScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_config: Option<ScheduleConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_filter: Option<EventFilter>,
}

/// Worktree mode for agent nodes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WorktreeMode {
    /// Creates new worktree/branch (default behavior)
    #[default]
    Own,
    /// Shares upstream agent's worktree (runs as child run under parent job)
    Inherit,
    /// No worktree (for read-only/analysis agents running on main)
    None,
}

impl std::fmt::Display for WorktreeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorktreeMode::Own => write!(f, "own"),
            WorktreeMode::Inherit => write!(f, "inherit"),
            WorktreeMode::None => write!(f, "none"),
        }
    }
}

impl std::str::FromStr for WorktreeMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "own" => Ok(WorktreeMode::Own),
            "inherit" => Ok(WorktreeMode::Inherit),
            "none" => Ok(WorktreeMode::None),
            _ => Err(format!("Unknown worktree mode: {}", s)),
        }
    }
}

/// Git configuration for agent nodes
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AgentGitConfig {
    /// Worktree mode: "own" (default), "inherit", or "none"
    #[serde(default, skip_serializing_if = "is_default_worktree_mode")]
    pub worktree_mode: WorktreeMode,
}

/// Helper to skip serializing default worktree_mode
fn is_default_worktree_mode(mode: &WorktreeMode) -> bool {
    *mode == WorktreeMode::Own
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentNodeConfig {
    pub agent_config_id: Option<String>,
    /// Output schema attached to agent (docks to right)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<SchemaConfig>,
    /// Git/worktree configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_config: Option<AgentGitConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionNodeConfig {
    /// Reference to action_configs table (new style)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_config_id: Option<String>,
    /// Legacy action string (e.g., "create_pr") - used for migration
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub action_params: serde_json::Value,
    /// Input schema attached above action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<SchemaConfig>,
    /// Output schema attached below action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<SchemaConfig>,
}

/// How a produced artifact's confirm gate resolves.
///
/// `Auto` flips `artifact.confirmed` true on the first write (the projection
/// derives Complete immediately); `User` leaves it false until a human confirms
/// in the UI (the projection derives Blocked while the latest artifact is
/// unconfirmed). A node with no declared schema defaults to `Auto`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConfirmPolicy {
    #[default]
    Auto,
    User,
}

/// Schema configuration for agent output or action input/output
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaConfig {
    /// The artifact's canonical name and `cairn:~/<name>` URI segment. One
    /// identifier — no separate display label, no preset reference.
    pub name: String,
    /// The full, self-contained JSON Schema for the artifact payload. Presets are
    /// baked in at authoring time (an editor convenience), never referenced at
    /// runtime. `None` means the field shape is inherited from a downstream
    /// context-edge target (e.g. a builder whose PR shape comes from `create_pr`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Whether the produced artifact auto-confirms or waits for a human. The
    /// producing node's gate decision, independent of any inherited field shape.
    #[serde(default)]
    pub confirm_policy: ConfirmPolicy,
    /// Custom tool name (defaults to "return")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    /// Tool description shown to the agent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Standalone checkpoint node config — a single pass-through command gate.
///
/// The command runs in the execution worktree: exit 0 passes (continue), a
/// non-zero exit blocks the checkpoint job (a resumable halt). There is no
/// human-approval checkpoint variant; a producing node's `confirm_policy` on its
/// output schema is the sole human-approval gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointNodeConfig {
    /// Command to run (exit 0 = pass/continue, non-zero = fail/block).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Optional prompt/message to show the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactNodeConfig {
    pub artifact_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
}

/// Condition type - how the condition is evaluated
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConditionType {
    #[default]
    Programmatic, // Expression evaluating artifact data
    Ai, // Haiku one-shot question
}

impl std::fmt::Display for ConditionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConditionType::Programmatic => write!(f, "programmatic"),
            ConditionType::Ai => write!(f, "ai"),
        }
    }
}

impl std::str::FromStr for ConditionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "programmatic" => Ok(ConditionType::Programmatic),
            "ai" => Ok(ConditionType::Ai),
            _ => Err(format!("Unknown condition type: {}", s)),
        }
    }
}

/// Error behavior for condition evaluation failures
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ConditionErrorBehavior {
    #[default]
    UseDefault, // Activate default_port on error
    Block, // Block execution, require manual intervention
}

impl std::fmt::Display for ConditionErrorBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConditionErrorBehavior::UseDefault => write!(f, "useDefault"),
            ConditionErrorBehavior::Block => write!(f, "block"),
        }
    }
}

impl std::str::FromStr for ConditionErrorBehavior {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "usedefault" | "use_default" => Ok(ConditionErrorBehavior::UseDefault),
            "block" => Ok(ConditionErrorBehavior::Block),
            _ => Err(format!("Unknown condition error behavior: {}", s)),
        }
    }
}

/// Condition node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionNodeConfig {
    pub condition_type: ConditionType,
    /// For programmatic: expression like "plan.risk_score > 7"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    /// For AI: question like "Does this change affect database schema?"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
    /// For AI: model to use (defaults to "haiku")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Output port names: ["yes", "no"] or ["low", "medium", "high"]
    pub ports: Vec<String>,
    /// Error handling behavior
    #[serde(default)]
    pub on_error: ConditionErrorBehavior,
    /// Default port to use on error (when on_error = UseDefault)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_port: Option<String>,
}

/// Context node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextNodeConfig {
    /// The markdown/text content
    pub content: String,
}

/// Recipe edge - a connection between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeEdge {
    pub id: String,
    pub edge_type: RecipeEdgeType,
    pub source_node_id: String,
    pub source_handle: String,
    pub target_node_id: String,
    pub target_handle: String,
}

/// Convert DbRecipeNode to RecipeNode
impl TryFrom<crate::db_records::DbRecipeNode> for RecipeNode {
    type Error = String;

    fn try_from(db: crate::db_records::DbRecipeNode) -> Result<Self, Self::Error> {
        let node_type: RecipeNodeType = db
            .node_type
            .parse()
            .map_err(|e: String| format!("Invalid node_type: {}", e))?;

        // Parse config JSON
        let config: Option<NodeConfig> = db
            .config
            .as_ref()
            .map(|s| serde_json::from_str(s))
            .transpose()
            .map_err(|e| format!("Invalid config JSON: {}", e))?;

        // Extract type-specific configs
        let (
            trigger_config,
            agent_config,
            action_config,
            checkpoint_config,
            artifact_config,
            condition_config,
            context_config,
        ) = if let Some(cfg) = config {
            // Build agent config with nested checkpoint/schema/git_config
            let agent_config = if cfg.agent_config_id.is_some()
                || cfg.output_schema.is_some()
                || cfg.git_config.is_some()
            {
                // Only create agent config if we have agent-related data
                if node_type == RecipeNodeType::Agent {
                    Some(AgentNodeConfig {
                        agent_config_id: cfg.agent_config_id,
                        output_schema: cfg.output_schema.clone(),
                        git_config: cfg.git_config,
                    })
                } else {
                    None
                }
            } else {
                None
            };

            // Build action config with nested schemas/checkpoint
            let action_config = if cfg.action.is_some()
                || cfg.action_config_id.is_some()
                || (node_type == RecipeNodeType::Pr && cfg.input_schema.is_some())
            {
                Some(ActionNodeConfig {
                    action_config_id: cfg.action_config_id,
                    action: cfg.action.unwrap_or_default(),
                    action_params: cfg
                        .action_params
                        .unwrap_or(serde_json::Value::Object(Default::default())),
                    input_schema: cfg.input_schema,
                    output_schema: if node_type == RecipeNodeType::Action {
                        cfg.output_schema
                    } else {
                        None
                    },
                })
            } else {
                None
            };

            // Build condition config
            let condition_config = if cfg.condition_type.is_some() || cfg.ports.is_some() {
                Some(ConditionNodeConfig {
                    condition_type: cfg.condition_type.unwrap_or_default(),
                    expression: cfg.expression,
                    question: cfg.question,
                    model: cfg.model,
                    ports: cfg.ports.unwrap_or_default(),
                    on_error: cfg.on_error.unwrap_or_default(),
                    default_port: cfg.default_port,
                })
            } else {
                None
            };

            (
                cfg.trigger_type.map(|t| TriggerConfig {
                    trigger_type: t,
                    scope: cfg.scope,
                    schedule_config: cfg.schedule_config,
                    event_filter: cfg.event_filter,
                }),
                agent_config,
                action_config,
                // Standalone checkpoint node config (command gate). Keyed on the
                // node type now that the command is the only checkpoint shape.
                if node_type == RecipeNodeType::Checkpoint {
                    Some(CheckpointNodeConfig {
                        command: cfg.command,
                        prompt: cfg.prompt,
                    })
                } else {
                    None
                },
                // Legacy standalone artifact config
                cfg.artifact_type.map(|at| ArtifactNodeConfig {
                    artifact_type: at,
                    schema: cfg.schema,
                }),
                condition_config,
                // Context config
                cfg.content.map(|content| ContextNodeConfig { content }),
            )
        } else {
            (None, None, None, None, None, None, None)
        };

        Ok(RecipeNode {
            id: db.id,
            node_type,
            name: db.name,
            position: NodePosition {
                x: db.position_x,
                y: db.position_y,
            },
            parent_id: db.parent_id,
            trigger_config,
            agent_config,
            action_config,
            checkpoint_config,
            artifact_config,
            condition_config,
            context_config,
        })
    }
}

/// Convert DbRecipeEdge to RecipeEdge
impl TryFrom<crate::db_records::DbRecipeEdge> for RecipeEdge {
    type Error = String;

    fn try_from(db: crate::db_records::DbRecipeEdge) -> Result<Self, Self::Error> {
        let edge_type: RecipeEdgeType = db
            .edge_type
            .parse()
            .map_err(|e: String| format!("Invalid edge_type: {}", e))?;

        Ok(RecipeEdge {
            id: db.id,
            edge_type,
            source_node_id: db.source_node_id,
            source_handle: db.source_handle,
            target_node_id: db.target_node_id,
            target_handle: db.target_handle,
        })
    }
}

/// Info about a recipe version (for version history display)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeVersionInfo {
    pub id: String,
    pub version: i32,
    pub created_at: i64,
    pub is_current: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_records::{DbRecipeEdge, DbRecipeNode};

    fn assert_parse_cases<T>(cases: &[(&str, T)])
    where
        T: std::str::FromStr + PartialEq + std::fmt::Debug + Clone,
        T::Err: std::fmt::Debug,
    {
        for (input, expected) in cases {
            assert_eq!(input.parse::<T>().unwrap(), expected.clone());
        }
    }

    // =========================================================================
    // RecipeTrigger tests
    // =========================================================================

    #[test]
    fn recipe_trigger_display() {
        assert_eq!(RecipeTrigger::Manual.to_string(), "manual");
        assert_eq!(RecipeTrigger::Schedule.to_string(), "schedule");
        assert_eq!(RecipeTrigger::JobEnded.to_string(), "job_ended");
        assert_eq!(RecipeTrigger::SkillCalled.to_string(), "skill_called");
    }

    #[test]
    fn recipe_enums_parse_canonical_values() {
        assert_parse_cases(&[
            ("manual", RecipeTrigger::Manual),
            ("schedule", RecipeTrigger::Schedule),
            ("job_ended", RecipeTrigger::JobEnded),
            ("skill_called", RecipeTrigger::SkillCalled),
        ]);
        assert_parse_cases(&[
            ("issue", TriggerScope::Issue),
            ("project", TriggerScope::Project),
        ]);
        assert_parse_cases(&[
            ("trigger", RecipeNodeType::Trigger),
            ("agent", RecipeNodeType::Agent),
            ("action", RecipeNodeType::Action),
            ("checkpoint", RecipeNodeType::Checkpoint),
            ("artifact", RecipeNodeType::Artifact),
            ("condition", RecipeNodeType::Condition),
            ("context", RecipeNodeType::Context),
        ]);
        assert_parse_cases(&[
            ("control", RecipeEdgeType::Control),
            ("context", RecipeEdgeType::Context),
        ]);
        assert_parse_cases(&[
            ("programmatic", ConditionType::Programmatic),
            ("ai", ConditionType::Ai),
        ]);
        assert_parse_cases(&[
            ("own", WorktreeMode::Own),
            ("inherit", WorktreeMode::Inherit),
            ("none", WorktreeMode::None),
        ]);
        assert_parse_cases(&[
            ("usedefault", ConditionErrorBehavior::UseDefault),
            ("block", ConditionErrorBehavior::Block),
        ]);
    }

    #[test]
    fn recipe_enums_parse_case_insensitively() {
        assert_parse_cases(&[
            ("MANUAL", RecipeTrigger::Manual),
            ("Schedule", RecipeTrigger::Schedule),
        ]);
        assert_parse_cases(&[
            ("TRIGGER", RecipeNodeType::Trigger),
            ("Agent", RecipeNodeType::Agent),
        ]);
        assert_parse_cases(&[
            ("CONTROL", RecipeEdgeType::Control),
            ("Context", RecipeEdgeType::Context),
        ]);
        assert_parse_cases(&[
            ("PROGRAMMATIC", ConditionType::Programmatic),
            ("AI", ConditionType::Ai),
        ]);
        assert_parse_cases(&[
            ("OWN", WorktreeMode::Own),
            ("Inherit", WorktreeMode::Inherit),
            ("NONE", WorktreeMode::None),
        ]);
        assert_parse_cases(&[
            ("USEDEFAULT", ConditionErrorBehavior::UseDefault),
            ("Block", ConditionErrorBehavior::Block),
        ]);
    }

    #[test]
    fn recipe_trigger_from_str_legacy_issue_maps_to_manual() {
        assert_eq!(
            "issue".parse::<RecipeTrigger>().unwrap(),
            RecipeTrigger::Manual
        );
        assert_eq!(
            "ISSUE".parse::<RecipeTrigger>().unwrap(),
            RecipeTrigger::Manual
        );
    }

    #[test]
    fn recipe_trigger_from_str_invalid() {
        let result = "invalid".parse::<RecipeTrigger>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown recipe trigger"));
    }

    #[test]
    fn recipe_trigger_default() {
        assert_eq!(RecipeTrigger::default(), RecipeTrigger::Manual);
    }

    #[test]
    fn recipe_trigger_serde_alias_issue() {
        // Deserializing "issue" from JSON/YAML should produce Manual
        let trigger: RecipeTrigger = serde_json::from_str("\"issue\"").unwrap();
        assert_eq!(trigger, RecipeTrigger::Manual);
    }

    // =========================================================================
    // TriggerScope tests
    // =========================================================================

    #[test]
    fn trigger_scope_display() {
        assert_eq!(TriggerScope::Issue.to_string(), "issue");
        assert_eq!(TriggerScope::Project.to_string(), "project");
    }

    #[test]
    fn trigger_scope_from_str_invalid() {
        let result = "invalid".parse::<TriggerScope>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown trigger scope"));
    }

    #[test]
    fn trigger_scope_default() {
        assert_eq!(TriggerScope::default(), TriggerScope::Issue);
    }

    // =========================================================================
    // Recipe::scope() tests
    // =========================================================================

    #[test]
    fn recipe_scope_returns_trigger_node_scope() {
        let recipe = Recipe {
            id: "r1".into(),
            name: "Test".into(),
            description: None,
            trigger: RecipeTrigger::Manual,
            workspace_id: None,
            project_id: None,
            is_system: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![RecipeNode {
                id: "t1".into(),
                name: "Trigger".into(),
                node_type: RecipeNodeType::Trigger,
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: Some(TriggerConfig {
                    trigger_type: RecipeTrigger::Manual,
                    scope: Some(TriggerScope::Project),
                    schedule_config: None,
                    event_filter: None,
                }),
                agent_config: None,
                action_config: None,
                artifact_config: None,
                checkpoint_config: None,
                condition_config: None,
                context_config: None,
            }],
            edges: vec![],
            created_at: 0,
            updated_at: 0,
        };
        assert_eq!(recipe.scope(), TriggerScope::Project);
    }

    #[test]
    fn recipe_scope_defaults_to_issue_when_no_trigger_node() {
        let recipe = Recipe {
            id: "r1".into(),
            name: "Test".into(),
            description: None,
            trigger: RecipeTrigger::Manual,
            workspace_id: None,
            project_id: None,
            is_system: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![],
            edges: vec![],
            created_at: 0,
            updated_at: 0,
        };
        assert_eq!(recipe.scope(), TriggerScope::Issue);
    }

    #[test]
    fn recipe_scope_defaults_to_issue_when_trigger_has_no_scope() {
        let recipe = Recipe {
            id: "r1".into(),
            name: "Test".into(),
            description: None,
            trigger: RecipeTrigger::Manual,
            workspace_id: None,
            project_id: None,
            is_system: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![RecipeNode {
                id: "t1".into(),
                name: "Trigger".into(),
                node_type: RecipeNodeType::Trigger,
                position: NodePosition { x: 0.0, y: 0.0 },
                parent_id: None,
                trigger_config: Some(TriggerConfig {
                    trigger_type: RecipeTrigger::Manual,
                    scope: None,
                    schedule_config: None,
                    event_filter: None,
                }),
                agent_config: None,
                action_config: None,
                artifact_config: None,
                checkpoint_config: None,
                condition_config: None,
                context_config: None,
            }],
            edges: vec![],
            created_at: 0,
            updated_at: 0,
        };
        assert_eq!(recipe.scope(), TriggerScope::Issue);
    }

    // =========================================================================
    // EventFilter tests
    // =========================================================================

    #[test]
    fn event_filter_serde_roundtrip() {
        let filter = EventFilter {
            job_status: Some(vec!["complete".into(), "failed".into()]),
            skill_ids: None,
            node_filter: Some(AgentFilter {
                mode: AgentFilterMode::Allow,
                ids: vec!["Builder".into()],
            }),
            every: None,
            group_by: None,
            accumulation_scope: None,
            time_window_secs: None,
        };
        let json = serde_json::to_string(&filter).unwrap();
        let parsed: EventFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed.job_status,
            Some(vec!["complete".into(), "failed".into()])
        );
        assert!(parsed.skill_ids.is_none());
        assert_eq!(
            parsed.node_filter,
            Some(AgentFilter {
                mode: AgentFilterMode::Allow,
                ids: vec!["Builder".into()],
            })
        );
    }

    #[test]
    fn event_filter_deserialize_old_string_format() {
        // Old format: nodeFilter was a plain string
        let json = r#"{"jobStatus":["complete"],"nodeFilter":"Builder"}"#;
        let parsed: EventFilter = serde_json::from_str(json).unwrap();
        assert_eq!(
            parsed.node_filter,
            Some(AgentFilter {
                mode: AgentFilterMode::Allow,
                ids: vec!["Builder".into()],
            })
        );
    }

    #[test]
    fn event_filter_deserialize_old_empty_string() {
        let json = r#"{"nodeFilter":""}"#;
        let parsed: EventFilter = serde_json::from_str(json).unwrap();
        assert!(parsed.node_filter.is_none());
    }

    #[test]
    fn event_filter_deserialize_new_struct_format() {
        let json = r#"{"nodeFilter":{"mode":"exclude","ids":["build","review"]}}"#;
        let parsed: EventFilter = serde_json::from_str(json).unwrap();
        assert_eq!(
            parsed.node_filter,
            Some(AgentFilter {
                mode: AgentFilterMode::Exclude,
                ids: vec!["build".into(), "review".into()],
            })
        );
    }

    #[test]
    fn event_filter_omits_none_fields() {
        let filter = EventFilter {
            job_status: None,
            skill_ids: Some(vec!["code-review".into()]),
            node_filter: None,
            every: None,
            group_by: None,
            accumulation_scope: None,
            time_window_secs: None,
        };
        let json = serde_json::to_string(&filter).unwrap();
        assert!(!json.contains("jobStatus"));
        assert!(json.contains("skillIds"));
        assert!(!json.contains("nodeFilter"));
    }

    // =========================================================================
    // RecipeNodeType tests
    // =========================================================================

    #[test]
    fn recipe_node_type_display() {
        assert_eq!(RecipeNodeType::Trigger.to_string(), "trigger");
        assert_eq!(RecipeNodeType::Agent.to_string(), "agent");
        assert_eq!(RecipeNodeType::Action.to_string(), "action");
        assert_eq!(RecipeNodeType::Checkpoint.to_string(), "checkpoint");
        assert_eq!(RecipeNodeType::Artifact.to_string(), "artifact");
        assert_eq!(RecipeNodeType::Condition.to_string(), "condition");
        assert_eq!(RecipeNodeType::Context.to_string(), "context");
    }

    #[test]
    fn recipe_node_type_from_str_invalid() {
        let result = "invalid".parse::<RecipeNodeType>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown recipe node type"));
    }

    #[test]
    fn recipe_node_type_executor_is_unknown() {
        let result = "executor".parse::<RecipeNodeType>();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unknown recipe node type: executor");
    }

    // =========================================================================
    // RecipeEdgeType tests
    // =========================================================================

    #[test]
    fn recipe_edge_type_display() {
        assert_eq!(RecipeEdgeType::Control.to_string(), "control");
        assert_eq!(RecipeEdgeType::Context.to_string(), "context");
    }

    #[test]
    fn recipe_edge_type_from_str_invalid() {
        let result = "invalid".parse::<RecipeEdgeType>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown recipe edge type"));
    }

    // =========================================================================
    // ConditionType tests
    // =========================================================================

    #[test]
    fn condition_type_display() {
        assert_eq!(ConditionType::Programmatic.to_string(), "programmatic");
        assert_eq!(ConditionType::Ai.to_string(), "ai");
    }

    #[test]
    fn condition_type_from_str_invalid() {
        let result = "invalid".parse::<ConditionType>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown condition type"));
    }

    #[test]
    fn condition_type_default() {
        assert_eq!(ConditionType::default(), ConditionType::Programmatic);
    }

    // =========================================================================
    // WorktreeMode tests
    // =========================================================================

    #[test]
    fn worktree_mode_display() {
        assert_eq!(WorktreeMode::Own.to_string(), "own");
        assert_eq!(WorktreeMode::Inherit.to_string(), "inherit");
        assert_eq!(WorktreeMode::None.to_string(), "none");
    }

    #[test]
    fn worktree_mode_from_str_invalid() {
        let result = "invalid".parse::<WorktreeMode>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown worktree mode"));
    }

    #[test]
    fn worktree_mode_default() {
        assert_eq!(WorktreeMode::default(), WorktreeMode::Own);
    }

    // =========================================================================
    // ConditionErrorBehavior tests
    // =========================================================================

    #[test]
    fn condition_error_behavior_display() {
        assert_eq!(ConditionErrorBehavior::UseDefault.to_string(), "useDefault");
        assert_eq!(ConditionErrorBehavior::Block.to_string(), "block");
    }

    #[test]
    fn condition_error_behavior_from_str_alternate_format() {
        // "use_default" is an alternate format
        assert_eq!(
            "use_default".parse::<ConditionErrorBehavior>().unwrap(),
            ConditionErrorBehavior::UseDefault
        );
    }

    #[test]
    fn condition_error_behavior_from_str_invalid() {
        let result = "invalid".parse::<ConditionErrorBehavior>();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Unknown condition error behavior"));
    }

    #[test]
    fn condition_error_behavior_default() {
        assert_eq!(
            ConditionErrorBehavior::default(),
            ConditionErrorBehavior::UseDefault
        );
    }

    // =========================================================================
    // TryFrom<DbRecipeNode> tests
    // =========================================================================

    fn make_db_recipe_node(node_type: &str, config: Option<&str>) -> DbRecipeNode {
        DbRecipeNode {
            id: "node-1".to_string(),
            recipe_id: "recipe-1".to_string(),
            node_type: node_type.to_string(),
            name: "Test Node".to_string(),
            position_x: 100.0,
            position_y: 200.0,
            config: config.map(|s| s.to_string()),
            created_at: 1000,
            updated_at: 2000,
            parent_id: None,
        }
    }

    #[test]
    fn try_from_db_recipe_node_trigger() {
        let config = r#"{"triggerType": "issue"}"#;
        let db = make_db_recipe_node("trigger", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.id, "node-1");
        assert_eq!(node.node_type, RecipeNodeType::Trigger);
        assert_eq!(node.name, "Test Node");
        assert_eq!(node.position.x, 100.0);
        assert_eq!(node.position.y, 200.0);
        assert!(node.trigger_config.is_some());
        assert_eq!(
            node.trigger_config.unwrap().trigger_type,
            RecipeTrigger::Manual
        );
    }

    #[test]
    fn try_from_db_recipe_node_agent() {
        let config = r#"{"agentConfigId": "agent-123"}"#;
        let db = make_db_recipe_node("agent", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Agent);
        assert!(node.agent_config.is_some());
        let agent_config = node.agent_config.unwrap();
        assert_eq!(agent_config.agent_config_id, Some("agent-123".to_string()));
    }

    #[test]
    fn try_from_db_recipe_node_agent_with_git_config() {
        let config = r#"{"agentConfigId": "agent-123", "gitConfig": {"worktreeMode": "inherit"}}"#;
        let db = make_db_recipe_node("agent", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        let agent_config = node.agent_config.unwrap();
        assert!(agent_config.git_config.is_some());
        let git_config = agent_config.git_config.unwrap();
        assert_eq!(git_config.worktree_mode, WorktreeMode::Inherit);
    }

    #[test]
    fn try_from_db_recipe_node_action() {
        let config = r#"{"action": "create_pr", "actionParams": {"title": "PR Title"}}"#;
        let db = make_db_recipe_node("action", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Action);
        assert!(node.action_config.is_some());
        let action_config = node.action_config.unwrap();
        assert_eq!(action_config.action, "create_pr");
        assert_eq!(
            action_config.action_params.get("title").unwrap(),
            "PR Title"
        );
    }

    #[test]
    fn try_from_db_recipe_node_checkpoint() {
        let config = r#"{"command": "npm test", "prompt": "CI"}"#;
        let db = make_db_recipe_node("checkpoint", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Checkpoint);
        assert!(node.checkpoint_config.is_some());
        let checkpoint_config = node.checkpoint_config.unwrap();
        assert_eq!(checkpoint_config.command, Some("npm test".to_string()));
        assert_eq!(checkpoint_config.prompt, Some("CI".to_string()));
    }

    #[test]
    fn try_from_db_recipe_node_artifact() {
        let config = r#"{"artifactType": "plan", "schema": {"type": "object"}}"#;
        let db = make_db_recipe_node("artifact", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Artifact);
        assert!(node.artifact_config.is_some());
        let artifact_config = node.artifact_config.unwrap();
        assert_eq!(artifact_config.artifact_type, "plan");
        assert!(artifact_config.schema.is_some());
    }

    #[test]
    fn try_from_db_recipe_node_condition() {
        let config =
            r#"{"conditionType": "ai", "question": "Is this ready?", "ports": ["yes", "no"]}"#;
        let db = make_db_recipe_node("condition", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Condition);
        assert!(node.condition_config.is_some());
        let condition_config = node.condition_config.unwrap();
        assert_eq!(condition_config.condition_type, ConditionType::Ai);
        assert_eq!(
            condition_config.question,
            Some("Is this ready?".to_string())
        );
        assert_eq!(condition_config.ports, vec!["yes", "no"]);
    }

    #[test]
    fn try_from_db_recipe_node_no_config() {
        let db = make_db_recipe_node("trigger", None);

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Trigger);
        assert!(node.trigger_config.is_none());
        assert!(node.agent_config.is_none());
        assert!(node.action_config.is_none());
    }

    #[test]
    fn try_from_db_recipe_node_invalid_node_type() {
        let db = make_db_recipe_node("invalid", None);

        let result = RecipeNode::try_from(db);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid node_type"));
    }

    #[test]
    fn try_from_db_recipe_node_invalid_config_json() {
        let db = make_db_recipe_node("trigger", Some("not valid json"));

        let result = RecipeNode::try_from(db);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid config JSON"));
    }

    #[test]
    fn try_from_db_recipe_node_with_parent_id() {
        let mut db = make_db_recipe_node("checkpoint", None);
        db.parent_id = Some("parent-node".to_string());

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.parent_id, Some("parent-node".to_string()));
    }

    // =========================================================================
    // TryFrom<DbRecipeEdge> tests
    // =========================================================================

    fn make_db_recipe_edge(edge_type: &str) -> DbRecipeEdge {
        DbRecipeEdge {
            id: "edge-1".to_string(),
            recipe_id: "recipe-1".to_string(),
            edge_type: edge_type.to_string(),
            source_node_id: "node-a".to_string(),
            source_handle: "ctrl-out".to_string(),
            target_node_id: "node-b".to_string(),
            target_handle: "ctrl-in".to_string(),
            created_at: 1000,
        }
    }

    #[test]
    fn try_from_db_recipe_edge_control() {
        let db = make_db_recipe_edge("control");

        let edge = RecipeEdge::try_from(db).unwrap();

        assert_eq!(edge.id, "edge-1");
        assert_eq!(edge.edge_type, RecipeEdgeType::Control);
        assert_eq!(edge.source_node_id, "node-a");
        assert_eq!(edge.source_handle, "ctrl-out");
        assert_eq!(edge.target_node_id, "node-b");
        assert_eq!(edge.target_handle, "ctrl-in");
    }

    #[test]
    fn try_from_db_recipe_edge_context() {
        let db = make_db_recipe_edge("context");

        let edge = RecipeEdge::try_from(db).unwrap();

        assert_eq!(edge.edge_type, RecipeEdgeType::Context);
    }

    #[test]
    fn try_from_db_recipe_edge_invalid_type() {
        let db = make_db_recipe_edge("invalid");

        let result = RecipeEdge::try_from(db);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid edge_type"));
    }
}
