//! Recipe file format for export/import.
//!
//! Provides a portable, human-readable format for sharing recipes.
//! Supports both YAML (primary) and JSON formats.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use super::{
    ActionNodeConfig, AgentGitConfig, AgentNodeConfig, ArtifactNodeConfig, CheckpointNodeConfig,
    ConditionErrorBehavior, ConditionNodeConfig, ConditionType, ContextNodeConfig, NodePosition,
    Recipe, RecipeContext, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeTrigger,
    ScheduleConfig, SchemaConfig, TriggerConfig, WebhookFilters, WorktreeMode,
};

/// Generate a slug from a name (lowercase, hyphens, no special chars)
fn slugify(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Generate unique readable IDs for nodes (UUID -> "planner-1")
fn generate_readable_ids(nodes: &[RecipeNode]) -> HashMap<String, String> {
    let mut id_map: HashMap<String, String> = HashMap::new();
    let mut slug_counts: HashMap<String, usize> = HashMap::new();

    for node in nodes {
        let slug = slugify(&node.name);
        let count = slug_counts.entry(slug.clone()).or_insert(0);
        *count += 1;
        let readable_id = format!("{}-{}", slug, count);
        id_map.insert(node.id.clone(), readable_id);
    }

    id_map
}

/// Current format version for future compatibility
pub const CURRENT_CAIRN_VERSION: u32 = 1;

/// Recipe file format for export/import
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeFile {
    /// Format version for future compatibility
    pub cairn_version: u32,
    /// Recipe name
    pub name: String,
    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// When the recipe can be triggered
    pub trigger: RecipeTrigger,
    /// Execution context
    pub context: RecipeContext,
    /// Node definitions
    pub nodes: Vec<RecipeFileNode>,
    /// Edge definitions
    pub edges: Vec<RecipeFileEdge>,
}

/// Compact node representation for file format
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeFileNode {
    /// Node identifier (used for edge references)
    pub id: String,
    /// Node type
    #[serde(rename = "type")]
    pub node_type: RecipeNodeType,
    /// Display name
    pub name: String,
    /// Position as "x@y" string format (e.g., "60@50")
    pub position: String,
    /// Optional parent node reference (for slot nodes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Type-specific configuration (flattened for readability)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<NodeFileConfig>,
}

/// Unified config structure for file format (more readable than separate configs)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeFileConfig {
    // Trigger config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_type: Option<RecipeTrigger>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_config: Option<ScheduleConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_filters: Option<WebhookFilters>,

    // Agent config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// Worktree mode: "own" (default), "inherit", or "none"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree_mode: Option<WorktreeMode>,

    // Action config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_config_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_params: Option<serde_json::Value>,

    // Common nested configs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointNodeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<SchemaConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<SchemaConfig>,

    // Standalone checkpoint config (for checkpoint nodes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_config: Option<CheckpointNodeConfig>,

    // Artifact config
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

/// Compact edge representation for file format
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeFileEdge {
    /// Optional edge ID (auto-generated on import if omitted)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Source node ID with optional handle suffix (e.g., "planner-1@control-out")
    pub from: String,
    /// Target node ID with optional handle suffix (e.g., "builder-1@control-in")
    pub to: String,
    /// Edge type (control or context)
    #[serde(rename = "type")]
    pub edge_type: RecipeEdgeType,
}

/// Validation result for recipe files
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipeFileValidation {
    /// Whether the file is valid for import
    pub valid: bool,
    /// Non-fatal warnings (e.g., unknown agent config references)
    pub warnings: Vec<String>,
    /// Fatal errors that prevent import
    pub errors: Vec<String>,
    /// Number of nodes in the file
    pub node_count: usize,
    /// Number of edges in the file
    pub edge_count: usize,
}

// ============================================================================
// Conversion: Recipe -> RecipeFile (Export)
// ============================================================================

impl From<Recipe> for RecipeFile {
    fn from(recipe: Recipe) -> Self {
        // Generate readable ID mapping (UUID -> "planner-1")
        let id_map = generate_readable_ids(&recipe.nodes);

        // Convert nodes with readable IDs
        let nodes: Vec<RecipeFileNode> = recipe
            .nodes
            .into_iter()
            .map(|node| {
                let readable_id = id_map
                    .get(&node.id)
                    .cloned()
                    .unwrap_or_else(|| node.id.clone());
                let readable_parent = node
                    .parent_id
                    .as_ref()
                    .and_then(|pid| id_map.get(pid).cloned());
                RecipeFileNode::from_node(node, readable_id, readable_parent)
            })
            .collect();

        // Deduplicate edges by full connection (source@handle, target@handle, type)
        let mut seen = HashSet::new();
        let edges: Vec<RecipeFileEdge> = recipe
            .edges
            .into_iter()
            .filter(|edge| {
                let key = (
                    edge.source_node_id.clone(),
                    edge.source_handle.clone(),
                    edge.target_node_id.clone(),
                    edge.target_handle.clone(),
                    edge.edge_type.clone(),
                );
                seen.insert(key)
            })
            .map(|edge| {
                let source_node = id_map
                    .get(&edge.source_node_id)
                    .cloned()
                    .unwrap_or(edge.source_node_id);
                let target_node = id_map
                    .get(&edge.target_node_id)
                    .cloned()
                    .unwrap_or(edge.target_node_id);
                // Use node@handle format for clarity
                let from = format!("{}@{}", source_node, edge.source_handle);
                let to = format!("{}@{}", target_node, edge.target_handle);
                RecipeFileEdge {
                    id: None, // Omit edge IDs in export
                    from,
                    to,
                    edge_type: edge.edge_type,
                }
            })
            .collect();

        RecipeFile {
            cairn_version: CURRENT_CAIRN_VERSION,
            name: recipe.name,
            description: recipe.description,
            trigger: recipe.trigger,
            context: recipe.context,
            nodes,
            edges,
        }
    }
}

impl RecipeFileNode {
    /// Create RecipeFileNode with explicit readable ID and parent_id
    fn from_node(node: RecipeNode, id: String, parent_id: Option<String>) -> Self {
        let config = build_node_file_config(&node);
        RecipeFileNode {
            id,
            node_type: node.node_type,
            name: node.name,
            position: format!(
                "{}@{}",
                node.position.x.round() as i32,
                node.position.y.round() as i32
            ),
            parent_id,
            config,
        }
    }
}

/// Build unified config from type-specific configs
fn build_node_file_config(node: &RecipeNode) -> Option<NodeFileConfig> {
    let mut config = NodeFileConfig::default();
    let mut has_config = false;

    // Trigger config
    if let Some(tc) = &node.trigger_config {
        config.trigger_type = Some(tc.trigger_type.clone());
        has_config = true;
    }

    // Agent config
    if let Some(ac) = &node.agent_config {
        config.agent = ac.agent_config_id.clone();
        config.checkpoint = ac.checkpoint.clone();
        config.output_schema = ac.output_schema.clone();
        // Export worktree_mode if non-default
        if let Some(ref gc) = ac.git_config {
            if gc.worktree_mode != WorktreeMode::Own {
                config.worktree_mode = Some(gc.worktree_mode.clone());
            }
        }
        has_config = true;
    }

    // Action config
    if let Some(ac) = &node.action_config {
        config.action_config_id = ac.action_config_id.clone();
        if !ac.action.is_empty() {
            config.action = Some(ac.action.clone());
        }
        if !ac.action_params.is_null() {
            config.action_params = Some(ac.action_params.clone());
        }
        config.input_schema = ac.input_schema.clone();
        // For action nodes, output_schema and checkpoint come from action_config
        if config.output_schema.is_none() {
            config.output_schema = ac.output_schema.clone();
        }
        if config.checkpoint.is_none() {
            config.checkpoint = ac.checkpoint.clone();
        }
        has_config = true;
    }

    // Standalone checkpoint config (for checkpoint nodes)
    if let Some(cc) = &node.checkpoint_config {
        config.checkpoint_config = Some(cc.clone());
        has_config = true;
    }

    // Artifact config
    if let Some(ac) = &node.artifact_config {
        config.artifact_type = Some(ac.artifact_type.clone());
        config.schema = ac.schema.clone();
        has_config = true;
    }

    // Condition config
    if let Some(cc) = &node.condition_config {
        config.condition_type = Some(cc.condition_type.clone());
        config.expression = cc.expression.clone();
        config.question = cc.question.clone();
        config.model = cc.model.clone();
        config.ports = Some(cc.ports.clone());
        config.on_error = Some(cc.on_error.clone());
        config.default_port = cc.default_port.clone();
        has_config = true;
    }

    // Context config
    if let Some(cc) = &node.context_config {
        config.content = Some(cc.content.clone());
        has_config = true;
    }

    if has_config {
        Some(config)
    } else {
        None
    }
}

// ============================================================================
// Conversion: RecipeFile -> Recipe (Import)
// ============================================================================

impl RecipeFile {
    /// Convert to Recipe for import, generating new IDs
    pub fn into_recipe(self, workspace_id: Option<String>, project_id: Option<String>) -> Recipe {
        let mut id_map: HashMap<String, String> = HashMap::new();

        // Generate new IDs for all nodes
        let nodes: Vec<RecipeNode> = self
            .nodes
            .into_iter()
            .map(|n| {
                let new_id = Uuid::new_v4().to_string();
                id_map.insert(n.id.clone(), new_id.clone());
                n.into_node(new_id, &id_map)
            })
            .collect();

        // Remap edge references
        let edges: Vec<RecipeEdge> = self
            .edges
            .into_iter()
            .map(|e| e.into_edge(&id_map))
            .collect();

        let now = chrono::Utc::now().timestamp();

        Recipe {
            id: Uuid::new_v4().to_string(),
            name: self.name,
            description: self.description,
            trigger: self.trigger,
            context: self.context,
            workspace_id,
            project_id,
            is_default: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes,
            edges,
            created_at: now,
            updated_at: now,
        }
    }

    /// Validate the recipe file without importing
    pub fn validate(&self) -> RecipeFileValidation {
        let mut warnings = Vec::new();
        let mut errors = Vec::new();

        // Check version compatibility
        if self.cairn_version > CURRENT_CAIRN_VERSION {
            errors.push(format!(
                "Recipe file version {} not supported. Maximum supported: {}",
                self.cairn_version, CURRENT_CAIRN_VERSION
            ));
        }

        // Collect node IDs
        let node_ids: HashSet<_> = self.nodes.iter().map(|n| n.id.as_str()).collect();

        // Validate edge references (parse node@handle format)
        for edge in &self.edges {
            let (source_node, _) = parse_node_handle(&edge.from);
            let (target_node, _) = parse_node_handle(&edge.to);
            if !node_ids.contains(source_node) {
                errors.push(format!(
                    "Edge references unknown source node: {}",
                    source_node
                ));
            }
            if !node_ids.contains(target_node) {
                errors.push(format!(
                    "Edge references unknown target node: {}",
                    target_node
                ));
            }
        }

        // Check for trigger node
        if !self
            .nodes
            .iter()
            .any(|n| n.node_type == RecipeNodeType::Trigger)
        {
            errors.push("Recipe must have a trigger node".to_string());
        }

        // Check for duplicate node IDs
        let mut seen_ids = HashSet::new();
        for node in &self.nodes {
            if !seen_ids.insert(&node.id) {
                errors.push(format!("Duplicate node ID: {}", node.id));
            }
        }

        // Validate parent_id references
        for node in &self.nodes {
            if let Some(parent_id) = &node.parent_id {
                if !node_ids.contains(parent_id.as_str()) {
                    errors.push(format!(
                        "Node '{}' references unknown parent node: {}",
                        node.name, parent_id
                    ));
                }
            }
        }

        // Warn about agent config references (may not exist in target workspace)
        // Also validate inherit mode constraints
        for node in &self.nodes {
            if let Some(config) = &node.config {
                if let Some(agent_id) = &config.agent {
                    warnings.push(format!(
                        "Node '{}' references agent config '{}' - verify it exists in your workspace",
                        node.name, agent_id
                    ));
                }
                if let Some(action_id) = &config.action_config_id {
                    warnings.push(format!(
                        "Node '{}' references action config '{}' - verify it exists in your workspace",
                        node.name, action_id
                    ));
                }
                // Check inherit mode has a valid upstream agent
                if let Some(WorktreeMode::Inherit) = &config.worktree_mode {
                    // Find incoming control edges to this node
                    let has_agent_parent = self.edges.iter().any(|edge| {
                        let (target_node, _) = parse_node_handle(&edge.to);
                        if target_node != node.id {
                            return false;
                        }
                        if edge.edge_type != RecipeEdgeType::Control {
                            return false;
                        }
                        // Check if source is an agent node
                        let (source_node, _) = parse_node_handle(&edge.from);
                        self.nodes
                            .iter()
                            .any(|n| n.id == source_node && n.node_type == RecipeNodeType::Agent)
                    });
                    if !has_agent_parent {
                        warnings.push(format!(
                            "Node '{}' uses worktree_mode 'inherit' but has no upstream agent node - ensure it follows an agent with worktree_mode 'own'",
                            node.name
                        ));
                    }
                }
            }
        }

        RecipeFileValidation {
            valid: errors.is_empty(),
            warnings,
            errors,
            node_count: self.nodes.len(),
            edge_count: self.edges.len(),
        }
    }
}

impl RecipeFileNode {
    /// Convert to RecipeNode with new ID
    fn into_node(self, new_id: String, id_map: &HashMap<String, String>) -> RecipeNode {
        let config = self.config.unwrap_or_default();

        // Build type-specific configs
        let trigger_config = config.trigger_type.map(|t| TriggerConfig {
            trigger_type: t,
            schedule_config: config.schedule_config.clone(),
            webhook_filters: config.webhook_filters.clone(),
        });

        let agent_config = if config.agent.is_some()
            || config.checkpoint.is_some()
            || config.output_schema.is_some()
            || config.worktree_mode.is_some()
        {
            // Only create agent config for agent nodes
            if self.node_type == RecipeNodeType::Agent {
                // Build git_config if worktree_mode is specified
                let git_config = config.worktree_mode.clone().map(|mode| AgentGitConfig {
                    worktree_mode: mode,
                });
                Some(AgentNodeConfig {
                    agent_config_id: config.agent,
                    checkpoint: config.checkpoint.clone(),
                    output_schema: config.output_schema.clone(),
                    git_config,
                })
            } else {
                None
            }
        } else {
            None
        };

        let action_config = if config.action.is_some() || config.action_config_id.is_some() {
            Some(ActionNodeConfig {
                action_config_id: config.action_config_id,
                action: config.action.unwrap_or_default(),
                action_params: config
                    .action_params
                    .unwrap_or(serde_json::Value::Object(Default::default())),
                input_schema: config.input_schema,
                output_schema: if self.node_type == RecipeNodeType::Action {
                    config.output_schema
                } else {
                    None
                },
                checkpoint: if self.node_type == RecipeNodeType::Action {
                    config.checkpoint
                } else {
                    None
                },
            })
        } else {
            None
        };

        let checkpoint_config = config.checkpoint_config;

        let artifact_config = config.artifact_type.map(|at| ArtifactNodeConfig {
            artifact_type: at,
            schema: config.schema,
        });

        // Condition config
        let condition_config = if config.condition_type.is_some() || config.ports.is_some() {
            Some(ConditionNodeConfig {
                condition_type: config.condition_type.unwrap_or_default(),
                expression: config.expression,
                question: config.question,
                model: config.model,
                ports: config.ports.unwrap_or_default(),
                on_error: config.on_error.unwrap_or_default(),
                default_port: config.default_port,
            })
        } else {
            None
        };

        // Context config
        let context_config = config.content.map(|content| ContextNodeConfig { content });

        // Remap parent_id if present
        let parent_id = self
            .parent_id
            .map(|pid| id_map.get(&pid).cloned().unwrap_or(pid));

        RecipeNode {
            id: new_id,
            node_type: self.node_type,
            name: self.name,
            position: parse_position(&self.position),
            parent_id,
            trigger_config,
            agent_config,
            action_config,
            checkpoint_config,
            artifact_config,
            condition_config,
            context_config,
        }
    }
}

/// Parse "x@y" position format into NodePosition
fn parse_position(s: &str) -> NodePosition {
    let parts: Vec<&str> = s.split('@').collect();
    if parts.len() == 2 {
        let x = parts[0].parse::<f32>().unwrap_or(0.0);
        let y = parts[1].parse::<f32>().unwrap_or(0.0);
        NodePosition { x, y }
    } else {
        NodePosition { x: 0.0, y: 0.0 }
    }
}

/// Parse "node-id@handle" format, returning (node_id, Some(handle)) or (node_id, None)
fn parse_node_handle(s: &str) -> (&str, Option<&str>) {
    match s.rsplit_once('@') {
        Some((node, handle)) => (node, Some(handle)),
        None => (s, None),
    }
}

impl RecipeFileEdge {
    /// Convert to RecipeEdge with remapped node references
    fn into_edge(self, id_map: &HashMap<String, String>) -> RecipeEdge {
        // Parse node@handle format
        let (source_node, source_handle_opt) = parse_node_handle(&self.from);
        let (target_node, target_handle_opt) = parse_node_handle(&self.to);

        // Remap node IDs
        let source_id = id_map
            .get(source_node)
            .cloned()
            .unwrap_or_else(|| source_node.to_string());
        let target_id = id_map
            .get(target_node)
            .cloned()
            .unwrap_or_else(|| target_node.to_string());

        // Use parsed handles or defaults based on edge type
        let source_handle =
            source_handle_opt
                .map(String::from)
                .unwrap_or_else(|| match self.edge_type {
                    RecipeEdgeType::Control => "control-out".to_string(),
                    RecipeEdgeType::Context => "context-out".to_string(),
                });
        let target_handle =
            target_handle_opt
                .map(String::from)
                .unwrap_or_else(|| match self.edge_type {
                    RecipeEdgeType::Control => "control-in".to_string(),
                    RecipeEdgeType::Context => "context-in".to_string(),
                });

        RecipeEdge {
            id: Uuid::new_v4().to_string(), // Always generate new UUID
            edge_type: self.edge_type,
            source_node_id: source_id,
            source_handle,
            target_node_id: target_id,
            target_handle,
        }
    }
}

// ============================================================================
// Serialization helpers
// ============================================================================

impl RecipeFile {
    /// Serialize to YAML format
    pub fn to_yaml(&self) -> Result<String, String> {
        serde_yaml::to_string(self).map_err(|e| format!("YAML serialization error: {}", e))
    }

    /// Serialize to JSON format
    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string_pretty(self).map_err(|e| format!("JSON serialization error: {}", e))
    }

    /// Parse from YAML format
    pub fn from_yaml(content: &str) -> Result<Self, String> {
        serde_yaml::from_str(content).map_err(|e| format!("YAML parse error: {}", e))
    }

    /// Parse from JSON format
    pub fn from_json(content: &str) -> Result<Self, String> {
        serde_json::from_str(content).map_err(|e| format!("JSON parse error: {}", e))
    }

    /// Parse from either format (auto-detect)
    pub fn parse(content: &str) -> Result<Self, String> {
        // Try JSON first (faster, stricter)
        if let Ok(file) = Self::from_json(content) {
            return Ok(file);
        }
        // Fall back to YAML
        Self::from_yaml(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_recipe() -> Recipe {
        Recipe {
            id: "test-recipe-id".to_string(),
            name: "Test Recipe".to_string(),
            description: Some("A test recipe".to_string()),
            trigger: RecipeTrigger::Issue,
            context: RecipeContext::Issue,
            workspace_id: Some("default".to_string()),
            project_id: None,
            is_default: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![
                RecipeNode {
                    id: "trigger-1".to_string(),
                    node_type: RecipeNodeType::Trigger,
                    name: "Start".to_string(),
                    position: NodePosition { x: 0.0, y: 100.0 },
                    parent_id: None,
                    trigger_config: Some(TriggerConfig {
                        trigger_type: RecipeTrigger::Issue,
                        schedule_config: None,
                        webhook_filters: None,
                    }),
                    agent_config: None,
                    action_config: None,
                    checkpoint_config: None,
                    artifact_config: None,
                    condition_config: None,
                    context_config: None,
                },
                RecipeNode {
                    id: "agent-1".to_string(),
                    node_type: RecipeNodeType::Agent,
                    name: "Plan Agent".to_string(),
                    position: NodePosition { x: 200.0, y: 100.0 },
                    parent_id: None,
                    trigger_config: None,
                    agent_config: Some(AgentNodeConfig {
                        agent_config_id: Some("plan".to_string()),
                        checkpoint: None,
                        output_schema: None,
                        git_config: None,
                    }),
                    action_config: None,
                    checkpoint_config: None,
                    artifact_config: None,
                    condition_config: None,
                    context_config: None,
                },
            ],
            edges: vec![RecipeEdge {
                id: "edge-1".to_string(),
                edge_type: RecipeEdgeType::Control,
                source_node_id: "trigger-1".to_string(),
                source_handle: "control-out".to_string(),
                target_node_id: "agent-1".to_string(),
                target_handle: "control-in".to_string(),
            }],
            created_at: 1000000,
            updated_at: 1000000,
        }
    }

    #[test]
    fn test_export_to_yaml() {
        let recipe = make_test_recipe();
        let file: RecipeFile = recipe.into();
        let yaml = file.to_yaml().unwrap();

        assert!(yaml.contains("cairnVersion: 1"));
        assert!(yaml.contains("name: Test Recipe"));
        assert!(yaml.contains("trigger: issue"));
    }

    #[test]
    fn test_export_to_json() {
        let recipe = make_test_recipe();
        let file: RecipeFile = recipe.into();
        let json = file.to_json().unwrap();

        assert!(json.contains("\"cairnVersion\": 1"));
        assert!(json.contains("\"name\": \"Test Recipe\""));
    }

    #[test]
    fn test_roundtrip_yaml() {
        let original = make_test_recipe();
        let file: RecipeFile = original.clone().into();
        let yaml = file.to_yaml().unwrap();

        let parsed = RecipeFile::from_yaml(&yaml).unwrap();
        let imported = parsed.into_recipe(Some("default".to_string()), None);

        // Names and structure should match (IDs will differ)
        assert_eq!(imported.name, original.name);
        assert_eq!(imported.nodes.len(), original.nodes.len());
        assert_eq!(imported.edges.len(), original.edges.len());
    }

    #[test]
    fn test_roundtrip_json() {
        let original = make_test_recipe();
        let file: RecipeFile = original.clone().into();
        let json = file.to_json().unwrap();

        let parsed = RecipeFile::from_json(&json).unwrap();
        let imported = parsed.into_recipe(Some("default".to_string()), None);

        assert_eq!(imported.name, original.name);
        assert_eq!(imported.nodes.len(), original.nodes.len());
        assert_eq!(imported.edges.len(), original.edges.len());
    }

    #[test]
    fn test_validation_valid_file() {
        let recipe = make_test_recipe();
        let file: RecipeFile = recipe.into();
        let validation = file.validate();

        assert!(validation.valid);
        assert!(validation.errors.is_empty());
        assert_eq!(validation.node_count, 2);
        assert_eq!(validation.edge_count, 1);
    }

    #[test]
    fn test_validation_missing_trigger() {
        let file = RecipeFile {
            cairn_version: 1,
            name: "No Trigger".to_string(),
            description: None,
            trigger: RecipeTrigger::Issue,
            context: RecipeContext::Issue,
            nodes: vec![RecipeFileNode {
                id: "agent-1".to_string(),
                node_type: RecipeNodeType::Agent,
                name: "Agent".to_string(),
                position: "0@0".to_string(),
                parent_id: None,
                config: None,
            }],
            edges: vec![],
        };

        let validation = file.validate();
        assert!(!validation.valid);
        assert!(validation.errors.iter().any(|e| e.contains("trigger node")));
    }

    #[test]
    fn test_validation_invalid_edge_reference() {
        let file = RecipeFile {
            cairn_version: 1,
            name: "Bad Edge".to_string(),
            description: None,
            trigger: RecipeTrigger::Issue,
            context: RecipeContext::Issue,
            nodes: vec![RecipeFileNode {
                id: "trigger-1".to_string(),
                node_type: RecipeNodeType::Trigger,
                name: "Start".to_string(),
                position: "0@0".to_string(),
                parent_id: None,
                config: None,
            }],
            edges: vec![RecipeFileEdge {
                id: None,
                from: "trigger-1@control-out".to_string(),
                to: "nonexistent@control-in".to_string(),
                edge_type: RecipeEdgeType::Control,
            }],
        };

        let validation = file.validate();
        assert!(!validation.valid);
        assert!(validation.errors.iter().any(|e| e.contains("nonexistent")));
    }

    #[test]
    fn test_edge_default_handles() {
        // Without @handle suffix, defaults are used based on edge type
        let edge = RecipeFileEdge {
            id: None,
            from: "a".to_string(),
            to: "b".to_string(),
            edge_type: RecipeEdgeType::Control,
        };

        let id_map = HashMap::new();
        let converted = edge.into_edge(&id_map);

        assert_eq!(converted.source_handle, "control-out");
        assert_eq!(converted.target_handle, "control-in");
    }

    #[test]
    fn test_edge_explicit_handles() {
        // With @handle suffix, explicit handles are used
        let edge = RecipeFileEdge {
            id: None,
            from: "a@custom-out".to_string(),
            to: "b@custom-in".to_string(),
            edge_type: RecipeEdgeType::Control,
        };

        let id_map = HashMap::new();
        let converted = edge.into_edge(&id_map);

        assert_eq!(converted.source_handle, "custom-out");
        assert_eq!(converted.target_handle, "custom-in");
    }

    #[test]
    fn test_auto_detect_format() {
        let recipe = make_test_recipe();
        let file: RecipeFile = recipe.into();

        // JSON
        let json = file.to_json().unwrap();
        let parsed_json = RecipeFile::parse(&json).unwrap();
        assert_eq!(parsed_json.name, "Test Recipe");

        // YAML
        let yaml = file.to_yaml().unwrap();
        let parsed_yaml = RecipeFile::parse(&yaml).unwrap();
        assert_eq!(parsed_yaml.name, "Test Recipe");
    }

    #[test]
    fn test_context_node_roundtrip() {
        let recipe = Recipe {
            id: "test-recipe".to_string(),
            name: "Context Test".to_string(),
            description: None,
            trigger: RecipeTrigger::Issue,
            context: RecipeContext::Issue,
            workspace_id: Some("default".to_string()),
            project_id: None,
            is_default: false,
            version: 1,
            parent_recipe_id: None,
            child_recipe_id: None,
            nodes: vec![
                RecipeNode {
                    id: "trigger-1".to_string(),
                    node_type: RecipeNodeType::Trigger,
                    name: "Start".to_string(),
                    position: NodePosition { x: 0.0, y: 0.0 },
                    parent_id: None,
                    trigger_config: Some(TriggerConfig {
                        trigger_type: RecipeTrigger::Issue,
                        schedule_config: None,
                        webhook_filters: None,
                    }),
                    agent_config: None,
                    action_config: None,
                    checkpoint_config: None,
                    artifact_config: None,
                    condition_config: None,
                    context_config: None,
                },
                RecipeNode {
                    id: "context-1".to_string(),
                    node_type: RecipeNodeType::Context,
                    name: "Instructions".to_string(),
                    position: NodePosition { x: 100.0, y: 200.0 },
                    parent_id: None,
                    trigger_config: None,
                    agent_config: None,
                    action_config: None,
                    checkpoint_config: None,
                    artifact_config: None,
                    condition_config: None,
                    context_config: Some(ContextNodeConfig {
                        content: "This is the context content.\n\nWith multiple lines.".to_string(),
                    }),
                },
            ],
            edges: vec![],
            created_at: 1000000,
            updated_at: 1000000,
        };

        // Export to YAML
        let file: RecipeFile = recipe.into();
        let yaml = file.to_yaml().unwrap();

        // Verify content is in the YAML
        assert!(yaml.contains("This is the context content"));

        // Import back
        let parsed = RecipeFile::from_yaml(&yaml).unwrap();
        let imported = parsed.into_recipe(Some("default".to_string()), None);

        // Find the context node and verify content
        let context_node = imported
            .nodes
            .iter()
            .find(|n| n.node_type == RecipeNodeType::Context)
            .expect("Context node should exist");
        let context_config = context_node
            .context_config
            .as_ref()
            .expect("Context config should exist");
        assert_eq!(
            context_config.content,
            "This is the context content.\n\nWith multiple lines."
        );
    }
}
