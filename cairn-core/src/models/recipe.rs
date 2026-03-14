//! Recipe types - workflow definitions.

use serde::{Deserialize, Serialize};

/// Recipe trigger - when a recipe can be started
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RecipeTrigger {
    #[default]
    Issue,
    Project,
    Manual,
    Schedule,
    Webhook,
}

impl std::fmt::Display for RecipeTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecipeTrigger::Issue => write!(f, "issue"),
            RecipeTrigger::Project => write!(f, "project"),
            RecipeTrigger::Manual => write!(f, "manual"),
            RecipeTrigger::Schedule => write!(f, "schedule"),
            RecipeTrigger::Webhook => write!(f, "webhook"),
        }
    }
}

impl std::str::FromStr for RecipeTrigger {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "issue" => Ok(RecipeTrigger::Issue),
            "project" => Ok(RecipeTrigger::Project),
            "manual" => Ok(RecipeTrigger::Manual),
            "schedule" => Ok(RecipeTrigger::Schedule),
            "webhook" => Ok(RecipeTrigger::Webhook),
            _ => Err(format!("Unknown recipe trigger: {}", s)),
        }
    }
}

/// Recipe context - where the recipe executes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RecipeContext {
    #[default]
    Issue,
    Project,
}

impl std::fmt::Display for RecipeContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecipeContext::Issue => write!(f, "issue"),
            RecipeContext::Project => write!(f, "project"),
        }
    }
}

impl std::str::FromStr for RecipeContext {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "issue" => Ok(RecipeContext::Issue),
            "project" => Ok(RecipeContext::Project),
            _ => Err(format!("Unknown recipe context: {}", s)),
        }
    }
}

/// Recipe - a workflow definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Recipe {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub trigger: RecipeTrigger,
    pub context: RecipeContext,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
    pub is_default: bool,
    pub version: i32,
    pub parent_recipe_id: Option<String>,
    pub child_recipe_id: Option<String>,
    pub nodes: Vec<RecipeNode>,
    pub edges: Vec<RecipeEdge>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Input for creating a recipe
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRecipe {
    pub name: String,
    pub description: Option<String>,
    pub trigger: Option<RecipeTrigger>,
    pub context: Option<RecipeContext>,
    pub workspace_id: Option<String>,
    pub project_id: Option<String>,
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
    pub context: Option<RecipeContext>,
    #[allow(dead_code)]
    pub workspace_id: Option<Option<String>>,
    #[allow(dead_code)]
    pub project_id: Option<Option<String>>,
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
    Checkpoint,
    Artifact,
    Condition,
    Context,
    Executor,
}

impl std::fmt::Display for RecipeNodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecipeNodeType::Trigger => write!(f, "trigger"),
            RecipeNodeType::Agent => write!(f, "agent"),
            RecipeNodeType::Action => write!(f, "action"),
            RecipeNodeType::Checkpoint => write!(f, "checkpoint"),
            RecipeNodeType::Artifact => write!(f, "artifact"),
            RecipeNodeType::Condition => write!(f, "condition"),
            RecipeNodeType::Context => write!(f, "context"),
            RecipeNodeType::Executor => write!(f, "executor"),
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
            "checkpoint" => Ok(RecipeNodeType::Checkpoint),
            "artifact" => Ok(RecipeNodeType::Artifact),
            "condition" => Ok(RecipeNodeType::Condition),
            "context" => Ok(RecipeNodeType::Context),
            "executor" => Ok(RecipeNodeType::Executor),
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
    pub schedule_config: Option<ScheduleConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_filters: Option<WebhookFilters>,

    // Agent config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_config_id: Option<String>,
    /// Nested checkpoint for agent nodes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointNodeConfig>,
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
    /// Nested checkpoint for action nodes (uses checkpoint field above)

    // Legacy checkpoint config (for standalone checkpoint nodes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_type: Option<CheckpointType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>, // For programmatic checkpoints
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

/// Webhook filters for webhook-triggered recipes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookFilters {
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerConfig {
    pub trigger_type: RecipeTrigger,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_config: Option<ScheduleConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_filters: Option<WebhookFilters>,
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
    /// Checkpoint attached to agent (docks below, replaces ctrl-out)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointNodeConfig>,
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
    /// Checkpoint attached below action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointNodeConfig>,
}

/// Schema configuration for agent output or action input/output
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaConfig {
    pub name: String,
    pub schema_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<SchemaField>>,
    /// Custom tool name (defaults to "return")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    /// Tool description shown to the agent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
}

/// Checkpoint type - how the checkpoint is evaluated
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointType {
    #[default]
    Approval, // Manual user approval
    Programmatic, // Run command, evaluate exit code
}

impl std::fmt::Display for CheckpointType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointType::Approval => write!(f, "approval"),
            CheckpointType::Programmatic => write!(f, "programmatic"),
        }
    }
}

impl std::str::FromStr for CheckpointType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "approval" => Ok(CheckpointType::Approval),
            "programmatic" => Ok(CheckpointType::Programmatic),
            // Legacy support
            "prompt" => Ok(CheckpointType::Approval),
            _ => Err(format!("Unknown checkpoint type: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointNodeConfig {
    pub checkpoint_type: CheckpointType,
    /// For programmatic checkpoints: command to run (exit 0 = approve, non-zero = reject)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Optional prompt/message to show user
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
impl TryFrom<crate::diesel_models::DbRecipeNode> for RecipeNode {
    type Error = String;

    fn try_from(db: crate::diesel_models::DbRecipeNode) -> Result<Self, Self::Error> {
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
                || cfg.checkpoint.is_some()
                || cfg.output_schema.is_some()
                || cfg.git_config.is_some()
            {
                // Only create agent config if we have agent-related data
                if node_type == RecipeNodeType::Agent {
                    Some(AgentNodeConfig {
                        agent_config_id: cfg.agent_config_id,
                        checkpoint: cfg.checkpoint.clone(),
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
            let action_config = if cfg.action.is_some() || cfg.action_config_id.is_some() {
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
                    checkpoint: if node_type == RecipeNodeType::Action {
                        cfg.checkpoint
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
                    schedule_config: cfg.schedule_config,
                    webhook_filters: cfg.webhook_filters,
                }),
                agent_config,
                action_config,
                // Legacy standalone checkpoint config
                cfg.checkpoint_type.map(|ct| CheckpointNodeConfig {
                    checkpoint_type: ct,
                    command: cfg.command,
                    prompt: cfg.prompt,
                }),
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
impl TryFrom<crate::diesel_models::DbRecipeEdge> for RecipeEdge {
    type Error = String;

    fn try_from(db: crate::diesel_models::DbRecipeEdge) -> Result<Self, Self::Error> {
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
    use crate::diesel_models::{DbRecipeEdge, DbRecipeNode};

    // =========================================================================
    // RecipeTrigger tests
    // =========================================================================

    #[test]
    fn recipe_trigger_display() {
        assert_eq!(RecipeTrigger::Issue.to_string(), "issue");
        assert_eq!(RecipeTrigger::Project.to_string(), "project");
        assert_eq!(RecipeTrigger::Manual.to_string(), "manual");
    }

    #[test]
    fn recipe_trigger_from_str() {
        assert_eq!(
            "issue".parse::<RecipeTrigger>().unwrap(),
            RecipeTrigger::Issue
        );
        assert_eq!(
            "project".parse::<RecipeTrigger>().unwrap(),
            RecipeTrigger::Project
        );
        assert_eq!(
            "manual".parse::<RecipeTrigger>().unwrap(),
            RecipeTrigger::Manual
        );
    }

    #[test]
    fn recipe_trigger_from_str_case_insensitive() {
        assert_eq!(
            "ISSUE".parse::<RecipeTrigger>().unwrap(),
            RecipeTrigger::Issue
        );
        assert_eq!(
            "Manual".parse::<RecipeTrigger>().unwrap(),
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
        assert_eq!(RecipeTrigger::default(), RecipeTrigger::Issue);
    }

    // =========================================================================
    // RecipeContext tests
    // =========================================================================

    #[test]
    fn recipe_context_display() {
        assert_eq!(RecipeContext::Issue.to_string(), "issue");
        assert_eq!(RecipeContext::Project.to_string(), "project");
    }

    #[test]
    fn recipe_context_from_str() {
        assert_eq!(
            "issue".parse::<RecipeContext>().unwrap(),
            RecipeContext::Issue
        );
        assert_eq!(
            "project".parse::<RecipeContext>().unwrap(),
            RecipeContext::Project
        );
    }

    #[test]
    fn recipe_context_from_str_case_insensitive() {
        assert_eq!(
            "ISSUE".parse::<RecipeContext>().unwrap(),
            RecipeContext::Issue
        );
        assert_eq!(
            "Project".parse::<RecipeContext>().unwrap(),
            RecipeContext::Project
        );
    }

    #[test]
    fn recipe_context_from_str_invalid() {
        let result = "invalid".parse::<RecipeContext>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown recipe context"));
    }

    #[test]
    fn recipe_context_default() {
        assert_eq!(RecipeContext::default(), RecipeContext::Issue);
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
    fn recipe_node_type_from_str() {
        assert_eq!(
            "trigger".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Trigger
        );
        assert_eq!(
            "agent".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Agent
        );
        assert_eq!(
            "action".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Action
        );
        assert_eq!(
            "checkpoint".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Checkpoint
        );
        assert_eq!(
            "artifact".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Artifact
        );
        assert_eq!(
            "condition".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Condition
        );
        assert_eq!(
            "context".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Context
        );
    }

    #[test]
    fn recipe_node_type_from_str_case_insensitive() {
        assert_eq!(
            "TRIGGER".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Trigger
        );
        assert_eq!(
            "Agent".parse::<RecipeNodeType>().unwrap(),
            RecipeNodeType::Agent
        );
    }

    #[test]
    fn recipe_node_type_from_str_invalid() {
        let result = "invalid".parse::<RecipeNodeType>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown recipe node type"));
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
    fn recipe_edge_type_from_str() {
        assert_eq!(
            "control".parse::<RecipeEdgeType>().unwrap(),
            RecipeEdgeType::Control
        );
        assert_eq!(
            "context".parse::<RecipeEdgeType>().unwrap(),
            RecipeEdgeType::Context
        );
    }

    #[test]
    fn recipe_edge_type_from_str_case_insensitive() {
        assert_eq!(
            "CONTROL".parse::<RecipeEdgeType>().unwrap(),
            RecipeEdgeType::Control
        );
        assert_eq!(
            "Context".parse::<RecipeEdgeType>().unwrap(),
            RecipeEdgeType::Context
        );
    }

    #[test]
    fn recipe_edge_type_from_str_invalid() {
        let result = "invalid".parse::<RecipeEdgeType>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown recipe edge type"));
    }

    // =========================================================================
    // CheckpointType tests
    // =========================================================================

    #[test]
    fn checkpoint_type_display() {
        assert_eq!(CheckpointType::Approval.to_string(), "approval");
        assert_eq!(CheckpointType::Programmatic.to_string(), "programmatic");
    }

    #[test]
    fn checkpoint_type_from_str() {
        assert_eq!(
            "approval".parse::<CheckpointType>().unwrap(),
            CheckpointType::Approval
        );
        assert_eq!(
            "programmatic".parse::<CheckpointType>().unwrap(),
            CheckpointType::Programmatic
        );
    }

    #[test]
    fn checkpoint_type_from_str_legacy_prompt() {
        // "prompt" is a legacy alias for "approval"
        assert_eq!(
            "prompt".parse::<CheckpointType>().unwrap(),
            CheckpointType::Approval
        );
    }

    #[test]
    fn checkpoint_type_from_str_case_insensitive() {
        assert_eq!(
            "APPROVAL".parse::<CheckpointType>().unwrap(),
            CheckpointType::Approval
        );
        assert_eq!(
            "Programmatic".parse::<CheckpointType>().unwrap(),
            CheckpointType::Programmatic
        );
    }

    #[test]
    fn checkpoint_type_from_str_invalid() {
        let result = "invalid".parse::<CheckpointType>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown checkpoint type"));
    }

    #[test]
    fn checkpoint_type_default() {
        assert_eq!(CheckpointType::default(), CheckpointType::Approval);
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
    fn condition_type_from_str() {
        assert_eq!(
            "programmatic".parse::<ConditionType>().unwrap(),
            ConditionType::Programmatic
        );
        assert_eq!("ai".parse::<ConditionType>().unwrap(), ConditionType::Ai);
    }

    #[test]
    fn condition_type_from_str_case_insensitive() {
        assert_eq!(
            "PROGRAMMATIC".parse::<ConditionType>().unwrap(),
            ConditionType::Programmatic
        );
        assert_eq!("AI".parse::<ConditionType>().unwrap(), ConditionType::Ai);
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
    fn worktree_mode_from_str() {
        assert_eq!("own".parse::<WorktreeMode>().unwrap(), WorktreeMode::Own);
        assert_eq!(
            "inherit".parse::<WorktreeMode>().unwrap(),
            WorktreeMode::Inherit
        );
        assert_eq!("none".parse::<WorktreeMode>().unwrap(), WorktreeMode::None);
    }

    #[test]
    fn worktree_mode_from_str_case_insensitive() {
        assert_eq!("OWN".parse::<WorktreeMode>().unwrap(), WorktreeMode::Own);
        assert_eq!(
            "Inherit".parse::<WorktreeMode>().unwrap(),
            WorktreeMode::Inherit
        );
        assert_eq!("NONE".parse::<WorktreeMode>().unwrap(), WorktreeMode::None);
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
    fn condition_error_behavior_from_str() {
        assert_eq!(
            "usedefault".parse::<ConditionErrorBehavior>().unwrap(),
            ConditionErrorBehavior::UseDefault
        );
        assert_eq!(
            "block".parse::<ConditionErrorBehavior>().unwrap(),
            ConditionErrorBehavior::Block
        );
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
    fn condition_error_behavior_from_str_case_insensitive() {
        assert_eq!(
            "USEDEFAULT".parse::<ConditionErrorBehavior>().unwrap(),
            ConditionErrorBehavior::UseDefault
        );
        assert_eq!(
            "Block".parse::<ConditionErrorBehavior>().unwrap(),
            ConditionErrorBehavior::Block
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
            RecipeTrigger::Issue
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
    fn try_from_db_recipe_node_agent_with_checkpoint() {
        let config = r#"{"agentConfigId": "agent-123", "checkpoint": {"checkpointType": "approval", "prompt": "Review?"}}"#;
        let db = make_db_recipe_node("agent", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        let agent_config = node.agent_config.unwrap();
        assert!(agent_config.checkpoint.is_some());
        let checkpoint = agent_config.checkpoint.unwrap();
        assert_eq!(checkpoint.checkpoint_type, CheckpointType::Approval);
        assert_eq!(checkpoint.prompt, Some("Review?".to_string()));
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
        let config = r#"{"checkpointType": "programmatic", "command": "npm test"}"#;
        let db = make_db_recipe_node("checkpoint", Some(config));

        let node = RecipeNode::try_from(db).unwrap();

        assert_eq!(node.node_type, RecipeNodeType::Checkpoint);
        assert!(node.checkpoint_config.is_some());
        let checkpoint_config = node.checkpoint_config.unwrap();
        assert_eq!(
            checkpoint_config.checkpoint_type,
            CheckpointType::Programmatic
        );
        assert_eq!(checkpoint_config.command, Some("npm test".to_string()));
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
