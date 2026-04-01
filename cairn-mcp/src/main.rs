use anyhow::Result;
use clap::Parser;
use rmcp::{
    handler::server::tool::{Parameters, ToolCallContext, ToolRouter},
    model::{
        Annotated, CallToolRequestParam, CallToolResult, Content, Implementation,
        ListResourcesResult, ListToolsResult, PaginatedRequestParam, ProtocolVersion, RawResource,
        ReadResourceRequestParam, ReadResourceResult, ResourceContents, ServerCapabilities,
        ServerInfo, Tool,
    },
    service::RequestContext,
    tool, tool_router, RoleServer, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cairn_common::uri::{parse_uri as parse_cairn_uri, CairnResource};

/// Cairn MCP Server - tools for Claude to interact with Cairn
#[derive(Parser)]
#[command(name = "cairn-mcp", version)]
struct Args {
    /// Run in external mode (limited toolset for use outside Cairn app)
    #[arg(long)]
    external: bool,

    /// Path to JSON Schema file for output tool
    #[arg(long)]
    schema: Option<String>,

    /// Custom name for the output tool (default: "return")
    #[arg(long)]
    tool_name: Option<String>,

    /// Custom description for the output tool
    #[arg(long)]
    tool_description: Option<String>,

    /// JSON-encoded list of available agents [{name, description}, ...]
    #[arg(long)]
    agents: Option<String>,

    /// JSON-encoded list of available skills [{id, name, description}, ...]
    #[arg(long)]
    skills: Option<String>,

    /// JSON-encoded list of custom tools [{id, name, description, inputSchema}, ...]
    #[arg(long)]
    tools: Option<String>,
}

/// Agent info for tool description
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentInfo {
    name: String,
    description: String,
}

/// Skill info for tool description
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SkillInfo {
    id: String,
    name: String,
    description: String,
}

impl SkillInfo {
    /// Format as a list item for tool descriptions.
    /// Shows `name (id): description` when name differs from id,
    /// otherwise just `id: description`.
    fn format_list_item(&self) -> String {
        let label = if !self.name.is_empty() && self.name.to_lowercase() != self.id.to_lowercase() {
            format!("{} (`{}`)", self.name, self.id)
        } else {
            self.id.clone()
        };
        format!("- {}: {}", label, self.description)
    }
}

/// Custom tool info for dynamic tool registration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolInfo {
    id: String,
    name: String,
    description: String,
    input_schema: serde_json::Value,
}

/// JSON Schema object type alias
type JsonSchemaObject = serde_json::Map<String, serde_json::Value>;

const MCP_CALLBACK_TIMEOUT_SECS: u64 = 600;

use cairn_common::protocol::{CallbackRequest, CallbackResponse};

/// Cairn MCP Server - tools for Claude to interact with Cairn during planning
#[derive(Clone)]
struct CairnMcp {
    callback_url: Arc<String>,
    /// Current working directory - used by backend to identify the active run
    cwd: Arc<String>,
    /// Run ID - preferred method to identify the active run (avoids cwd ambiguity)
    run_id: Option<Arc<String>>,
    /// Shared secret (base64-encoded string from env var, sent directly as bearer token)
    mcp_secret: Option<Arc<String>>,
    /// Whether running in external mode (limited toolset)
    external_mode: bool,
    tool_router: ToolRouter<Self>,
    /// Optional JSON Schema for the output tool (loaded from --schema arg)
    artifact_schema: Option<Arc<JsonSchemaObject>>,
    /// Custom name for the output tool (default: "return")
    output_tool_name: String,
    /// Custom description for the output tool
    output_tool_description: Option<String>,
    /// Available agents for task tool description
    available_agents: Vec<AgentInfo>,
    /// Available skills for skill tool description
    available_skills: Vec<SkillInfo>,
    /// Available custom tools (dynamic tools defined by user)
    available_tools: Vec<ToolInfo>,
    /// Rate limiter for bug reports (max 3 per session)
    report_count: Arc<AtomicU32>,
    web_cache: Arc<Mutex<WebCache>>,
    web_rate_limiter: Arc<Mutex<RateLimiter>>,
}

/// A single option for a question
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QuestionOption {
    /// The display text for this option (1-5 words)
    label: String,
    /// Explanation of what this option means
    description: String,
}

/// A single question to ask the user
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct Question {
    /// The complete question to ask (should end with ?)
    question: String,
    /// Optional short label for the question (max 12 chars)
    header: Option<String>,
    /// The available choices (2+ options). "Other" is automatically added.
    options: Vec<QuestionOption>,
    /// Set to true to allow multiple selections
    multi_select: bool,
}

/// Input for ask_user tool
///
/// Manual JsonSchema impl to produce a flat, inline schema without $ref.
/// Codex's sanitize_json_schema doesn't resolve nested schemars references,
/// so the derived schema for Question / QuestionOption loses structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AskUserInput {
    /// The questions to ask the user
    questions: Vec<Question>,
}

fn task_input_schema_value() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["description", "prompt", "subagentType"],
        "additionalProperties": false,
        "properties": {
            "description": {
                "type": "string",
                "description": "A short (3-5 word) description of the task"
            },
            "prompt": {
                "type": "string",
                "description": "The task for the agent to perform"
            },
            "subagentType": {
                "type": "string",
                "description": "The type of specialized agent to use (agent config ID)"
            },
            "tier": {
                "type": "string",
                "description": "Optional capability tier override (for example: \"sm\", \"md\", \"lg\", or qualified refs like \"codex/lg\"). Unqualified tiers resolve against `backend` when provided, otherwise the caller's current backend."
            },
            "backend": {
                "type": "string",
                "description": "Optional backend override for unqualified tiers (for example: \"claude\" or \"codex\"). Defaults to the caller's current backend."
            },
            "runInBackground": {
                "type": "boolean",
                "description": "Set to true to run this agent in the background"
            },
            "session": {
                "type": "string",
                "enum": ["new", "fork"],
                "description": "Optional delegated session strategy. Use \"fork\" to fork from the parent session; defaults to \"new\"."
            }
        }
    })
}

impl schemars::JsonSchema for AskUserInput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "AskUserInput".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value::<schemars::Schema>(serde_json::json!({
            "type": "object",
            "required": ["questions"],
            "additionalProperties": false,
            "properties": {
                "questions": {
                    "description": "The questions to ask the user",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["question", "options", "multiSelect"],
                        "additionalProperties": false,
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "The complete question to ask (should end with ?)"
                            },
                            "header": {
                                "type": "string",
                                "description": "Optional short label for the question (max 12 chars)"
                            },
                            "options": {
                                "type": "array",
                                "description": "The available choices (2+ options). \"Other\" is automatically added.",
                                "items": {
                                    "type": "object",
                                    "required": ["label", "description"],
                                    "additionalProperties": false,
                                    "properties": {
                                        "label": {
                                            "type": "string",
                                            "description": "The option label"
                                        },
                                        "description": {
                                            "type": "string",
                                            "description": "Explanation of what this option means"
                                        }
                                    }
                                }
                            },
                            "multiSelect": {
                                "type": "boolean",
                                "description": "Set to true to allow multiple selections"
                            }
                        }
                    }
                }
            }
        }))
        .unwrap()
    }
}

/// Input for add_comment tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct AddCommentInput {
    /// The comment content (supports markdown)
    content: String,
}

/// Input for message tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct MessageInput {
    /// Message content
    content: String,
    /// Target cairn:// URI. Determines scope:
    /// cairn://PROJECT → project, cairn://PROJECT/NUMBER → issue,
    /// cairn://PROJECT/NUMBER/EXEC/NODE → direct.
    /// Omit for project channel.
    to: Option<String>,
}

/// A single todo item in the task list (serde only — schema is manual to avoid $ref)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TodoWriteItemInput {
    id: Option<String>,
    content: String,
    status: String,
    priority: Option<String>,
    active_form: Option<String>,
}

/// Input for todo_write tool. Each call replaces the entire todo list.
///
/// Manual JsonSchema impl to produce a flat, inline schema without $ref.
/// Codex's sanitize_json_schema doesn't resolve $ref, so schemars-generated
/// references to named types (enums, structs) get coerced to "type": "string".
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TodoWriteInput {
    todos: Vec<TodoWriteItemInput>,
}

impl schemars::JsonSchema for TodoWriteInput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TodoWriteInput".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value::<schemars::Schema>(serde_json::json!({
            "type": "object",
            "required": ["todos"],
            "properties": {
                "todos": {
                    "description": "The complete todo list. Each call replaces the previous list — always send ALL items with current statuses.",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["content", "status"],
                        "properties": {
                            "id": {
                                "type": "string",
                                "description": "Stable identifier for this todo (e.g. \"setup-db\"). Helps track items across updates."
                            },
                            "content": {
                                "type": "string",
                                "description": "Imperative description of what needs to be done (e.g. \"Add database migration\")"
                            },
                            "status": {
                                "type": "string",
                                "enum": ["pending", "in_progress", "completed"],
                                "description": "pending = not started, in_progress = actively working, completed = done"
                            },
                            "priority": {
                                "type": "string",
                                "enum": ["high", "medium", "low"],
                                "description": "Optional priority level"
                            },
                            "activeForm": {
                                "type": "string",
                                "description": "Present-continuous form shown in UI while in_progress (e.g. \"Adding database migration\")"
                            }
                        }
                    }
                }
            }
        }))
        .unwrap()
    }
}

/// A single file change in an edit batch
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileChangeItem {
    /// File path relative to working directory
    path: String,
    /// Operation kind: "add" (new file), "update" (apply diff or find/replace), "delete" (remove file)
    kind: String,
    /// Full file content (required for kind=add)
    #[serde(default)]
    content: Option<String>,
    /// Unified diff for kind=update (alternative to old_string/new_string)
    #[serde(default)]
    diff: Option<String>,
    /// Text to find for kind=update (alternative to diff, used with new_string)
    #[serde(default)]
    old_string: Option<String>,
    /// Replacement text for kind=update (used with old_string)
    #[serde(default)]
    new_string: Option<String>,
    /// Replace all occurrences for old_string/new_string (default: false, first match only)
    #[serde(default)]
    replace_all: Option<bool>,
}

/// Input for unified edit tool.
/// Manual JsonSchema impl to produce a flat, inline schema without $ref.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileChangeInput {
    /// Array of file changes to apply atomically
    changes: Vec<FileChangeItem>,
    /// Git commit message. Use "^" to amend previous commit.
    commit_msg: String,
}

impl schemars::JsonSchema for FileChangeInput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FileChangeInput".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value::<schemars::Schema>(serde_json::json!({
            "type": "object",
            "required": ["changes", "commit_msg"],
            "properties": {
                "changes": {
                    "description": "Array of file changes to apply atomically",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["path", "kind"],
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "File path relative to working directory"
                            },
                            "kind": {
                                "type": "string",
                                "enum": ["add", "update", "delete"],
                                "description": "add=create new file, update=edit existing file, delete=remove file"
                            },
                            "content": {
                                "type": "string",
                                "description": "Full file content (required for kind=add)"
                            },
                            "diff": {
                                "type": "string",
                                "description": "Unified diff for kind=update. Alternative to old_string/new_string."
                            },
                            "old_string": {
                                "type": "string",
                                "description": "Text to find and replace for kind=update. Use with new_string. Supports ~~~~~ wildcard: skips content until a matching delimiter closes. Write the anchor text up to an opening delimiter, then ~~~~~} (or ~~~~~] / ~~~~~)) to match the depth-correct closer. Nested delimiters are skipped automatically. Example: 'post(\'/confirm\', async (c) => {~~~~~}' matches from the anchor through the depth-matched closing }. Alternative to diff."
                            },
                            "new_string": {
                                "type": "string",
                                "description": "Replacement text for kind=update. Required when old_string is provided."
                            },
                            "replace_all": {
                                "type": "boolean",
                                "description": "Replace all occurrences of old_string (default: false, first match only)"
                            }
                        }
                    }
                },
                "commit_msg": {
                    "type": "string",
                    "description": "Git commit message. Use '^' to amend previous commit. Use 'NO_COMMIT' to skip git."
                }
            }
        }))
        .unwrap()
    }
}

/// Input for read_uri tool (external-mode friendly, cairn:// URIs only)
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ReadUriInput {
    /// Cairn resource URI (e.g., cairn://PROJECT/NUMBER, cairn://PROJECT/NUMBER/EXEC/NODE/chat)
    uri: String,
    /// The line number to start reading from. Only provide if the content is too large to read at once.
    offset: Option<usize>,
    /// The number of lines to read. Only provide if the content is too large to read at once.
    limit: Option<usize>,
}

/// Input for read tool (mirrors native Read)
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct ReadFileInput {
    /// Path to read - a file path, directory path, or resource URI (cairn://)
    path: String,
    /// The line number to start reading from. Only provide if the file is too large to read at once.
    offset: Option<usize>,
    /// The number of lines to read. Only provide if the file is too large to read at once.
    limit: Option<usize>,
    /// Include issue history for this file. Set to true or "minimal" for brief history,
    /// "verbose" for detailed output including PR links and change stats.
    #[serde(default)]
    issue_history: Option<IssueHistoryMode>,
}

/// Mode for issue history output
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
enum IssueHistoryMode {
    /// Brief history with issue numbers and dates
    Minimal,
    /// Detailed history with PR links and change stats
    Verbose,
}

impl Default for IssueHistoryMode {
    fn default() -> Self {
        Self::Minimal
    }
}

/// Response from backend when reading an image file
#[derive(Debug, Clone, Deserialize)]
struct ImageResponse {
    is_image: bool,
    mime_type: String,
    data: String,
}

/// Input for create_issue tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct CreateIssueInput {
    /// Issue title
    title: String,
    /// Issue description (markdown)
    description: Option<String>,
    /// Skill IDs to attach to this issue
    skills: Option<Vec<String>>,
    /// Project key to create the issue in (e.g., "CAIRN"). Defaults to current project.
    project: Option<String>,
}

/// Input for update_issue tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct UpdateIssueInput {
    /// Issue number (e.g., 37 or "CAIRN-37")
    issue_number: String,
    /// New title (optional)
    title: Option<String>,
    /// New description (optional)
    description: Option<String>,
    /// Skill IDs to attach to this issue (replaces existing skills)
    skills: Option<Vec<String>>,
}

/// Input for search tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SearchInput {
    /// Search query - supports phrases "like this", prefix matching, and multiple terms
    query: String,
    /// Filter by content types: 'issue', 'comment', 'artifact', 'event'
    content_types: Option<Vec<String>>,
    /// Filter to specific project ID (default: current project)
    project_id: Option<String>,
    /// Filter to specific issue ID
    issue_id: Option<String>,
    /// Only include results after this Unix timestamp
    since: Option<i64>,
    /// Maximum results to return (default: 20, max: 100)
    limit: Option<usize>,
}

/// Input for bash tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BashInput {
    /// The command to execute
    command: String,
    /// Short description of what this command does (5-10 words)
    description: Option<String>,
    /// Timeout in milliseconds (default: 120000, max: 600000)
    timeout: Option<u32>,
    /// Run in background - spawns a visible terminal tab
    run_in_background: Option<bool>,
    /// Optional commit message. If provided, stages all changes and commits after command completes.
    /// Use "^" to amend the previous commit.
    commit_msg: Option<String>,
}

/// Input for kill_shell tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct KillShellInput {
    /// Terminal to kill - can be a slug (e.g., "dev-server") or full URI (e.g., "cairn://PROJ/123/builder-1/terminal/dev-server")
    terminal: String,
}

/// Input for task tool (matches native Task schema)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskInput {
    /// A short (3-5 word) description of the task
    description: String,
    /// The task for the agent to perform
    prompt: String,
    /// The type of specialized agent to use (agent config ID)
    subagent_type: String,
    /// Optional capability tier override (e.g. "sm", "md", "lg", "codex/lg").
    /// Unqualified tiers resolve against `backend` when provided, else caller backend.
    #[serde(default, alias = "model")]
    tier: Option<String>,
    /// Optional backend override for unqualified tiers (e.g. "claude" or "codex"); defaults to caller backend.
    #[serde(default, rename = "backend", alias = "backendPreference")]
    backend_preference: Option<String>,
    /// Set to true to run this agent in the background
    run_in_background: Option<bool>,
    /// Optional delegated session strategy. Use "fork" to fork from the parent session; defaults to "new".
    session: Option<String>,
    /// Task index for batch_tasks ordering (0, 1, 2...) - set internally by batch_tasks
    #[serde(skip_serializing_if = "Option::is_none")]
    task_index: Option<i32>,
}

impl schemars::JsonSchema for TaskInput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TaskInput".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value::<schemars::Schema>(task_input_schema_value()).unwrap()
    }
}

/// Input for batch_tasks tool - execute multiple tasks in parallel.
///
/// Manual JsonSchema impl to produce a flat, inline schema without $ref.
/// Codex's sanitize_json_schema doesn't resolve $ref, so nested task items
/// from schemars get coerced to `"type": "string"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchTasksInput {
    /// Array of tasks to execute in parallel
    tasks: Vec<TaskInput>,
}

impl schemars::JsonSchema for BatchTasksInput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "BatchTasksInput".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value::<schemars::Schema>(serde_json::json!({
            "type": "object",
            "required": ["tasks"],
            "additionalProperties": false,
            "properties": {
                "tasks": {
                    "description": "Array of tasks to execute in parallel",
                    "type": "array",
                    "items": task_input_schema_value()
                }
            }
        }))
        .unwrap()
    }
}

/// Input for skill tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SkillInput {
    /// Skill ID or name to retrieve
    #[serde(alias = "skillId")]
    skill: String,
}

/// Input for list_skills tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ListSkillsInput {
    /// Filter by scope: "workspace", "project", or "all" (default: all)
    #[serde(default)]
    scope: Option<String>,
}

/// Input for create_skill tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CreateSkillInput {
    /// Skill name (must be valid slug: lowercase, hyphens, 1-64 chars)
    name: String,
    /// Skill description (max 1024 chars)
    description: String,
    /// SKILL.md body (prompt content)
    prompt: String,
    /// "workspace" or "project" (default: "workspace")
    #[serde(default)]
    scope: Option<String>,
    /// Tool restrictions for this skill
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
    /// Model preference (written as metadata.model)
    #[serde(default)]
    model: Option<String>,
    /// Source issue (auto-derived from run context if omitted)
    #[serde(default)]
    source_issue: Option<String>,
}

/// Section replacement for update_skill
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ReplaceSectionInput {
    /// Markdown heading to find (e.g., "## Details")
    heading: String,
    /// New content for the section
    content: String,
}

/// Input for update_skill tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct UpdateSkillInput {
    /// Skill ID (name/slug)
    id: String,
    /// New description (optional)
    #[serde(default)]
    description: Option<String>,
    /// Full prompt body replacement (optional)
    #[serde(default)]
    prompt: Option<String>,
    /// Append to existing prompt body (optional)
    #[serde(default)]
    append_to_prompt: Option<String>,
    /// Replace a markdown section by heading (optional)
    #[serde(default)]
    replace_section: Option<ReplaceSectionInput>,
    /// Update allowed tools (optional)
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
    /// Update model (optional)
    #[serde(default)]
    model: Option<String>,
    /// Source issue for provenance
    #[serde(default)]
    source_issue: Option<String>,
}

/// Input for delete_skill tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DeleteSkillInput {
    /// Skill ID (name/slug)
    id: String,
    /// Reason for deletion (optional)
    #[serde(default)]
    reason: Option<String>,
}

/// Input for execute tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ExecuteInput {
    /// JavaScript/TypeScript code to execute in Bun runtime
    code: String,
    /// Timeout in seconds (default: 300, max: 600)
    #[serde(default)]
    timeout: Option<u32>,
}

/// Input for permission_prompt tool (used by Claude CLI --permission-prompt-tool)
/// Note: Claude CLI sends snake_case field names (tool_name, input)
/// tool_use_id may not be sent by Claude CLI
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct PermissionPromptInput {
    /// Unique identifier for this tool invocation (may not be provided by Claude CLI)
    #[serde(default)]
    tool_use_id: Option<String>,
    /// Name of the tool requesting permission
    tool_name: String,
    /// The complete input parameters the tool will receive
    input: serde_json::Value,
}

/// Input for list_memories tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ListMemoriesInput {
    /// Filter to active memories only (default: true)
    #[serde(default = "default_true")]
    active_only: bool,
}

fn default_true() -> bool {
    true
}

/// Trigger condition for creating a memory
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TriggerCondition {
    /// Group index: conditions with same index are ANDed, different indices are ORed
    trigger_index: i32,
    /// JSONPath into hook stdin (e.g., "$.tool_name", "$.tool_input.file_path")
    json_path: String,
    /// Regex pattern to match against the extracted value
    pattern: String,
}

/// Input for create_memory tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CreateMemoryInput {
    /// The memory content (1-3 sentences)
    content: String,
    /// Confidence level: "tentative" (default) or "established"
    #[serde(default)]
    confidence: Option<String>,
    /// Source issue identifier (e.g., "CAIRN-456")
    #[serde(default)]
    source_issue: Option<String>,
    /// Trigger conditions for when this memory should surface
    #[serde(default)]
    triggers: Option<Vec<TriggerCondition>>,
    /// Scope: "project" (default) or "branch:<name>" for branch-scoped memories
    #[serde(default)]
    scope: Option<String>,
    /// Keywords for substring matching (case-insensitive). Memory surfaces when any keyword appears.
    #[serde(default)]
    keywords: Option<Vec<String>>,
    /// Source run ID for provenance tracking. Auto-populated from the current run if omitted.
    #[serde(default)]
    source_run_id: Option<String>,
}

/// Input for update_memory tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct UpdateMemoryInput {
    /// The memory ID to update
    id: String,
    /// New content (optional)
    #[serde(default)]
    content: Option<String>,
    /// New confidence level (optional)
    #[serde(default)]
    confidence: Option<String>,
    /// Set to false to deactivate (optional)
    #[serde(default)]
    active: Option<bool>,
    /// Replacement trigger conditions (optional — replaces all existing triggers)
    #[serde(default)]
    triggers: Option<Vec<TriggerCondition>>,
    /// Scope: "project" (default) or "branch:<name>" for branch-scoped memories
    #[serde(default)]
    scope: Option<String>,
    /// Keywords for substring matching (case-insensitive). Memory surfaces when any keyword appears.
    #[serde(default)]
    keywords: Option<Vec<String>>,
}

/// Input for deactivate_memory tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DeactivateMemoryInput {
    /// The memory ID to deactivate
    id: String,
}

/// Input for bug_report tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BugReportInput {
    /// Category: tool_bug, prompt_issue, harness_friction, or suggestion
    category: String,
    /// Short summary of the issue
    title: String,
    /// Detailed description. Code snippets and file paths are fine for clarity,
    /// but do not include personally identifiable information (usernames, emails, API keys, etc.)
    description: String,
    /// Which tool was involved (optional)
    tool_name: Option<String>,
}

/// Input for glob tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct GlobInput {
    /// The glob pattern to match files against (e.g. "**/*.ts", "src/**/*.rs")
    pattern: String,
    /// Directory to search in. Defaults to the working directory.
    path: Option<String>,
}

/// Input for grep tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct GrepInput {
    /// Regex pattern to search for in file contents. This tool already uses ripgrep (rg) under the hood.
    pattern: String,
    /// File or directory to search in. Defaults to working directory.
    path: Option<String>,
    /// Glob pattern to filter files (e.g. "*.js", "*.{ts,tsx}")
    glob: Option<String>,
    /// File type filter (e.g. "js", "py", "rust")
    #[serde(rename = "type")]
    file_type: Option<String>,
    /// Output mode: "content", "files_with_matches" (default), or "count"
    output_mode: Option<String>,
    /// Lines of context before and after each match
    context: Option<u32>,
    /// Lines to show after each match
    #[serde(rename = "-A")]
    after_context: Option<u32>,
    /// Lines to show before each match
    #[serde(rename = "-B")]
    before_context: Option<u32>,
    /// Alias for context
    #[serde(rename = "-C")]
    context_alias: Option<u32>,
    /// Case insensitive search
    #[serde(rename = "-i")]
    case_insensitive: Option<bool>,
    /// Show line numbers (default: true for content mode)
    #[serde(rename = "-n")]
    line_numbers: Option<bool>,
    /// Limit output to first N lines/entries
    head_limit: Option<u32>,
    /// Skip first N lines/entries before applying head_limit
    offset: Option<u32>,
    /// Enable multiline matching
    multiline: Option<bool>,
}

/// Input for web_fetch tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct WebFetchInput {
    /// URL to fetch
    url: String,
    /// What to extract or look for in the page content
    prompt: String,
    /// Skip interpretation, return raw markdown (default: false)
    #[serde(default)]
    raw: Option<bool>,
    /// Max chars in result (default: 60000)
    #[serde(default)]
    max_length: Option<usize>,
}

/// Input for web_search tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct WebSearchInput {
    /// The search query to use
    query: String,
    /// Max results (default: 10)
    #[serde(default)]
    limit: Option<usize>,
    /// Only include search results from these domains
    #[serde(default)]
    allowed_domains: Option<Vec<String>>,
    /// Never include search results from these domains
    #[serde(default)]
    blocked_domains: Option<Vec<String>>,
}

// ============================================================================
// Web Cache
// ============================================================================

/// Cache entry with TTL tracking
struct WebCacheEntry {
    result: String,
    inserted_at: Instant,
}

/// In-memory URL→result cache with 15-minute TTL and LRU eviction
struct WebCache {
    entries: HashMap<(String, u64), WebCacheEntry>,
    max_entries: usize,
    ttl_secs: u64,
}

impl WebCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            max_entries: 100,
            ttl_secs: 15 * 60, // 15 minutes
        }
    }

    fn get(&mut self, url: &str, prompt_hash: u64) -> Option<&str> {
        let key = (url.to_string(), prompt_hash);
        // Check expiry first
        if let Some(entry) = self.entries.get(&key) {
            if entry.inserted_at.elapsed().as_secs() < self.ttl_secs {
                return self.entries.get(&key).map(|e| e.result.as_str());
            } else {
                self.entries.remove(&key);
                return None;
            }
        }
        None
    }

    fn put(&mut self, url: &str, prompt_hash: u64, result: String) {
        // Evict expired entries first
        let ttl = self.ttl_secs;
        self.entries
            .retain(|_, v| v.inserted_at.elapsed().as_secs() < ttl);

        // LRU eviction if at capacity: remove oldest entry
        if self.entries.len() >= self.max_entries {
            if let Some(oldest_key) = self
                .entries
                .iter()
                .min_by_key(|(_, v)| v.inserted_at)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&oldest_key);
            }
        }

        self.entries.insert(
            (url.to_string(), prompt_hash),
            WebCacheEntry {
                result,
                inserted_at: Instant::now(),
            },
        );
    }
}

// ============================================================================
// Rate Limiter
// ============================================================================

/// Per-domain rate limiter: max 10 requests per minute
struct RateLimiter {
    requests: HashMap<String, Vec<Instant>>,
    max_per_minute: usize,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            requests: HashMap::new(),
            max_per_minute: 10,
        }
    }

    /// Check if a request to the given domain is allowed.
    /// Returns Ok(()) if allowed, Err(seconds_to_wait) if rate limited.
    fn check(&mut self, domain: &str) -> Result<(), u64> {
        let now = Instant::now();
        let window = std::time::Duration::from_secs(60);

        let timestamps = self.requests.entry(domain.to_string()).or_default();

        // Remove timestamps older than 1 minute
        timestamps.retain(|t| now.duration_since(*t) < window);

        if timestamps.len() >= self.max_per_minute {
            // Find how long until the oldest request expires
            if let Some(oldest) = timestamps.first() {
                let elapsed = now.duration_since(*oldest).as_secs();
                return Err(60u64.saturating_sub(elapsed));
            }
            return Err(60);
        }

        timestamps.push(now);
        Ok(())
    }
}

// ============================================================================
// HTML Processing
// ============================================================================

/// Simple hash for cache key generation
fn hash_prompt(prompt: &str) -> u64 {
    let mut hash: u64 = 5381;
    for byte in prompt.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
    }
    hash
}

/// Strip boilerplate tags from HTML before markdown conversion.
/// Removes script, style, nav, footer, header elements to reduce noise.
fn strip_boilerplate(html: &str) -> String {
    let mut result = html.to_string();
    // Remove these tags and their contents using simple regex-like matching
    for tag in &["script", "style", "nav", "footer", "header", "noscript"] {
        loop {
            let open = format!("<{}", tag);
            let close = format!("</{}>", tag);

            if let Some(start) = result.to_lowercase().find(&open) {
                if let Some(end_offset) = result[start..].to_lowercase().find(&close) {
                    let end = start + end_offset + close.len();
                    result.replace_range(start..end, "");
                } else {
                    // No closing tag found, remove from open tag to end of self-closing or just the tag
                    if let Some(gt) = result[start..].find('>') {
                        result.replace_range(start..start + gt + 1, "");
                    } else {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }
    result
}

/// Extract a simple title from HTML
fn extract_title(html: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let start = lower.find("<title")?;
    let gt = html[start..].find('>')? + start + 1;
    let end = lower[gt..].find("</title>")? + gt;
    let title = html[gt..end].trim().to_string();
    if title.is_empty() {
        None
    } else {
        Some(title)
    }
}

/// Score a text section by keyword overlap with the prompt.
/// Returns a score: higher means more relevant.
fn score_section(section: &str, keywords: &[&str]) -> usize {
    let lower = section.to_lowercase();
    keywords
        .iter()
        .filter(|kw| lower.contains(&kw.to_lowercase()))
        .count()
}

/// Smart truncation: split content by headings, score sections by keyword overlap,
/// keep highest-scoring sections up to the char limit.
fn smart_truncate(content: &str, prompt: &str, max_chars: usize) -> String {
    if content.len() <= max_chars {
        return content.to_string();
    }

    // Extract keywords from prompt (words > 2 chars)
    let keywords: Vec<&str> = prompt.split_whitespace().filter(|w| w.len() > 2).collect();

    if keywords.is_empty() {
        // No meaningful keywords, just truncate
        return content[..max_chars].to_string();
    }

    // Split by markdown headings
    let mut sections: Vec<(String, usize)> = Vec::new();
    let mut current_section = String::new();

    for line in content.lines() {
        if line.starts_with('#') && !current_section.is_empty() {
            let score = score_section(&current_section, &keywords);
            sections.push((current_section, score));
            current_section = String::new();
        }
        current_section.push_str(line);
        current_section.push('\n');
    }
    if !current_section.is_empty() {
        let score = score_section(&current_section, &keywords);
        sections.push((current_section, score));
    }

    // If only one section, just truncate
    if sections.len() <= 1 {
        return content[..max_chars].to_string();
    }

    // Sort by score descending, keeping original order for equal scores
    let mut indexed: Vec<(usize, &str, usize)> = sections
        .iter()
        .enumerate()
        .map(|(i, (text, score))| (i, text.as_str(), *score))
        .collect();
    indexed.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));

    // Collect highest-scoring sections until we hit the limit
    let mut selected: Vec<(usize, &str)> = Vec::new();
    let mut total_len = 0;

    for (idx, text, _score) in &indexed {
        if total_len + text.len() > max_chars && !selected.is_empty() {
            break;
        }
        selected.push((*idx, text));
        total_len += text.len();
    }

    // Sort selected by original position to maintain document order
    selected.sort_by_key(|(idx, _)| *idx);

    let mut result: String = selected.iter().map(|(_, text)| *text).collect();

    // Final truncation if still too long
    if result.len() > max_chars {
        result.truncate(max_chars);
    }

    result
}

/// Convert HTML to markdown using htmd
fn html_to_markdown(html: &str) -> String {
    let cleaned = strip_boilerplate(html);
    htmd::convert(&cleaned).unwrap_or_else(|_| {
        // Fallback: just strip all HTML tags
        let mut result = String::new();
        let mut in_tag = false;
        for ch in cleaned.chars() {
            match ch {
                '<' => in_tag = true,
                '>' => in_tag = false,
                _ if !in_tag => result.push(ch),
                _ => {}
            }
        }
        result
    })
}

/// Normalize a URL: upgrade HTTP to HTTPS, validate format
fn normalize_url(url_str: &str) -> Result<url::Url, String> {
    let mut s = url_str.to_string();

    // Upgrade http to https
    if s.starts_with("http://") {
        s = format!("https://{}", &s[7..]);
    }

    // Add https:// if no scheme
    if !s.starts_with("https://") && !s.starts_with("http://") {
        s = format!("https://{}", s);
    }

    url::Url::parse(&s).map_err(|e| format!("Invalid URL: {}", e))
}

/// Extract domain from a URL
fn extract_domain(url: &url::Url) -> String {
    url.host_str().unwrap_or("unknown").to_string()
}

/// Response format from execute/custom_tool handlers.
/// Contains formatted output and error flag for MCP tool results.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolResponse {
    output: String,
    is_error: bool,
}

/// Max chars in a tool result (~20k tokens, safely under Claude Code's 25k token limit)
const MAX_RESULT_CHARS: usize = 45_000;

/// If text exceeds MAX_RESULT_CHARS, truncate at a line boundary and append continuation info.
/// Redact common secret patterns from a command string for safe logging.
fn redact_command(command: &str) -> String {
    use std::sync::LazyLock;

    static PATTERNS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
        vec![
            regex::Regex::new(r#"(?i)(Bearer\s+)[^\s'"]+"#).unwrap(),
            regex::Regex::new(r"\bsk[-_][a-zA-Z0-9._-]{8,}\b").unwrap(),
            regex::Regex::new(r"(?i)(export\s+[A-Z_]*(?:KEY|SECRET|TOKEN|PASSWORD)\s*=)\S+")
                .unwrap(),
            regex::Regex::new(r"(?i)(--password[= ])\S+").unwrap(),
        ]
    });

    let mut result = command.to_string();
    for pattern in PATTERNS.iter() {
        result = pattern
            .replace_all(&result, |caps: &regex::Captures| {
                if let Some(prefix) = caps.get(1) {
                    format!("{}[REDACTED]", prefix.as_str())
                } else {
                    "[REDACTED]".to_string()
                }
            })
            .to_string();
    }
    result
}

fn cap_text_result(text: &str, offset: usize) -> String {
    if text.len() <= MAX_RESULT_CHARS {
        return text.to_string();
    }

    // Find a safe byte position on a char boundary (avoids panic on multi-byte UTF-8)
    let mut safe_end = MAX_RESULT_CHARS;
    while safe_end > 0 && !text.is_char_boundary(safe_end) {
        safe_end -= 1;
    }

    // Find last newline before the safe limit
    let truncation_point = text[..safe_end].rfind('\n').unwrap_or(safe_end);

    let truncated = &text[..truncation_point];
    let lines_shown = truncated.lines().count();
    let total_lines = text.lines().count();
    let next_offset = offset + lines_shown;

    format!(
        "{}\n\n--- truncated (lines {}-{} of {}, {} of {} chars) ---\nCall again with offset={} to continue.",
        truncated,
        offset + 1, offset + lines_shown, total_lines,
        truncation_point, text.len(),
        next_offset
    )
}

/// Apply line-based offset/limit to text, then cap the result.
fn paginate_text(text: &str, offset: Option<usize>, limit: Option<usize>) -> String {
    let offset = offset.unwrap_or(0);
    let lines: Vec<&str> = text.lines().collect();

    if lines.is_empty() {
        return text.to_string();
    }

    if offset >= lines.len() {
        return format!(
            "Offset {} is past end of content ({} lines).",
            offset,
            lines.len()
        );
    }

    let start = offset;
    let end = match limit {
        Some(n) => (start + n).min(lines.len()),
        None => lines.len(),
    };

    let sliced = lines[start..end].join("\n");
    cap_text_result(&sliced, offset)
}

/// Parse a ToolResponse from a callback result string.
/// Falls back to treating the raw string as successful output.
/// Applies cap_text_result to prevent exceeding Claude Code's token limit.
fn parse_tool_response(raw: &str) -> CallToolResult {
    match serde_json::from_str::<ToolResponse>(raw) {
        Ok(resp) if resp.is_error => {
            CallToolResult::error(vec![Content::text(cap_text_result(&resp.output, 0))])
        }
        Ok(resp) => CallToolResult::success(vec![Content::text(cap_text_result(&resp.output, 0))]),
        Err(_) => {
            // Fallback: treat raw string as successful text output
            CallToolResult::success(vec![Content::text(cap_text_result(raw, 0))])
        }
    }
}

#[tool_router]
impl CairnMcp {
    fn new(
        callback_url: String,
        cwd: String,
        run_id: Option<String>,
        mcp_secret: Option<String>,
        external_mode: bool,
        artifact_schema: Option<JsonSchemaObject>,
        output_tool_name: Option<String>,
        output_tool_description: Option<String>,
        available_agents: Vec<AgentInfo>,
        available_skills: Vec<SkillInfo>,
        available_tools: Vec<ToolInfo>,
    ) -> Self {
        Self {
            callback_url: Arc::new(callback_url),
            cwd: Arc::new(cwd),
            run_id: run_id.map(Arc::new),
            mcp_secret: mcp_secret.map(|s| Arc::new(s)),
            external_mode,
            tool_router: Self::tool_router(),
            artifact_schema: artifact_schema.map(Arc::new),
            output_tool_name: output_tool_name.unwrap_or_else(|| "return".to_string()),
            output_tool_description,
            available_agents,
            available_skills,
            available_tools,
            report_count: Arc::new(AtomicU32::new(0)),
            web_cache: Arc::new(Mutex::new(WebCache::new())),
            web_rate_limiter: Arc::new(Mutex::new(RateLimiter::new())),
        }
    }

    /// Check if a tool is available in external mode.
    /// Returns an error message if the tool is internal-only and we're in external mode.
    fn require_internal(&self, tool_name: &str) -> Result<(), String> {
        if self.external_mode {
            Err(format!(
                "{} is not available in external mode. This tool requires the full Cairn app context.",
                tool_name
            ))
        } else {
            Ok(())
        }
    }

    /// Returns an error message if the tool is external-only and we're in internal mode.
    fn require_external(&self, tool_name: &str) -> Result<(), String> {
        if !self.external_mode {
            Err(format!(
                "{} is only available in external mode. Internal agents should use the 'read' tool instead.",
                tool_name
            ))
        } else {
            Ok(())
        }
    }

    /// Read a resource URI and return tool result.
    /// Handles cairn:// URI scheme. Applies optional line-based pagination.
    async fn read_resource_uri(
        &self,
        uri: &str,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        // Only cairn:// scheme is supported
        let tool_name = if uri.starts_with("cairn://") {
            match parse_cairn_uri(uri) {
                Some(resource) => {
                    match resource {
                        // Terminal resources use read_resource
                        CairnResource::NodeTerminal { .. }
                        | CairnResource::ProjectTerminal { .. } => "read_resource",
                        // All other resources use read_issue_resource
                        _ => "read_issue_resource",
                    }
                }
                None => {
                    return Ok(CallToolResult::success(vec![Content::text(format!(
                        "Invalid cairn resource URI: {}",
                        uri
                    ))]));
                }
            }
        } else {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Unknown resource scheme (expected cairn://): {}",
                uri
            ))]));
        };

        // Call Tauri callback
        let callback_request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: tool_name.to_string(),
            payload: serde_json::json!({ "uri": uri }),
            tool_use_id: None,
        };

        let result = self.call_tauri(&callback_request).await;

        // For terminal resources, parse the structured response
        if tool_name == "read_resource" {
            if let Ok(terminal_result) = serde_json::from_str::<TerminalReadResult>(&result) {
                let text = paginate_text(&terminal_result.output, offset, limit);
                return Ok(CallToolResult::success(vec![Content::text(text)]));
            }
        }

        // For issue resources (or fallback), apply pagination and cap
        let text = paginate_text(&result, offset, limit);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    /// Ask the user a question and wait for their response.
    /// Use this when you need clarification or want to confirm an approach before proceeding.
    #[tool(description = "Ask the user a question and wait for their response")]
    async fn ask_user(
        &self,
        params: Parameters<AskUserInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("ask_user") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("ask_user called with {} questions", input.questions.len());

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "ask_user".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        // Call Tauri and wait for response (blocks until user responds)
        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Add a comment to the current issue.
    /// Use this to record discoveries, status updates, or important notes during investigation.
    /// Comments are visible to the user and persist across sessions.
    #[tool(
        description = "Add a comment to the current issue for notes, status updates, or discoveries"
    )]
    async fn add_comment(
        &self,
        params: Parameters<AddCommentInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("add_comment called with {} chars", input.content.len());

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "add_comment".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Send a message to other agents. Messages arrive passively via hook.
    /// Direct messages can resume idle agents.
    #[tool(
        description = "Send a message to other agents via project channel, issue channel, or direct"
    )]
    async fn message(
        &self,
        params: Parameters<MessageInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("message") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!(
            "message called: to={:?}, {} chars",
            input.to,
            input.content.len()
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "message".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Create and manage a structured task list. Each call replaces the full list.
    /// Each todo has: content (string), status ("pending"|"in_progress"|"completed"),
    /// and optional activeForm (present-tense description shown while in_progress).
    #[tool(
        description = "Create and manage a structured task list. Each call sends the FULL list (replaces previous). Each todo object needs: content (string), status (\"pending\", \"in_progress\", or \"completed\"), and optionally activeForm (present-tense label shown during execution, e.g. \"Running tests\"). Mark tasks in_progress before starting, completed when done."
    )]
    async fn todo_write(
        &self,
        params: Parameters<TodoWriteInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("todo_write") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("todo_write called with {} items", input.todos.len());

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "todo_write".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Apply file changes and commit. Supports add, update (diff or find/replace with optional replace_all), and delete.
    #[tool(
        description = "Apply file changes and commit. Each change specifies a file path and operation kind.\n\nFor kind=update, two modes:\n- old_string + new_string: find/replace (optional replace_all). Supports ~~~~~ wildcard: skips content until a matching delimiter closes — write the anchor up to an opening delimiter, then ~~~~~} to match the depth-correct closer.\n- diff: unified diff\n\nFor kind=add: provide content (full file).\nFor kind=delete: just path and kind."
    )]
    async fn edit(
        &self,
        params: Parameters<FileChangeInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("edit") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!(
            "edit called: {} changes, msg: {}",
            input.changes.len(),
            if input.commit_msg == "^" {
                "amend"
            } else {
                &input.commit_msg
            }
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "edit".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Read file contents or resource URIs.
    /// Use this to examine files in the implementation worktree or read issue/terminal resources.
    #[tool(
        description = "Read file contents, directory listings, or resource URIs. Supports:\n- File paths: text files and images (PNG, JPEG, etc.)\n- Directory paths: lists contents with sizes\n- cairn:// URIs: issue details, comments, transcripts, artifacts, terminal output"
    )]
    async fn read(
        &self,
        params: Parameters<ReadFileInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("read") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("read called: {}", input.path);

        // Check for resource URI scheme
        if input.path.starts_with("cairn://") {
            return self
                .read_resource_uri(&input.path, input.offset, input.limit)
                .await;
        }

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "read".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;

        // Check if the response is an image
        match serde_json::from_str::<ImageResponse>(&result) {
            Ok(img) if img.is_image => {
                tracing::info!(
                    "Returning image content: mime_type={}, data_len={}",
                    img.mime_type,
                    img.data.len()
                );
                return Ok(CallToolResult::success(vec![Content::image(
                    img.data,
                    img.mime_type,
                )]));
            }
            Ok(_) => {
                tracing::debug!("ImageResponse parsed but is_image=false");
            }
            Err(e) => {
                tracing::debug!("Not an image response: {}", e);
            }
        }

        // Fall through to text content (cap in case file read exceeds limit)
        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&result, input.offset.unwrap_or(0)),
        )]))
    }

    /// Read a Cairn resource URI. Available in external mode.
    /// Supports issue details, node transcripts, artifacts, and more.
    #[tool(
        description = "Read Cairn resource URIs for issue details, transcripts, artifacts, and more. Use cairn:// URIs from search results."
    )]
    async fn read_uri(
        &self,
        params: Parameters<ReadUriInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_external("read_uri") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("read_uri called: {}", input.uri);
        self.read_resource_uri(&input.uri, input.offset, input.limit)
            .await
    }

    /// Search across issues, comments, artifacts, and past conversations.
    /// Returns ranked results with snippets and URIs for navigation.
    #[tool(
        description = "Search across issues, comments, plans, and past conversations. Returns ranked results with snippets and URIs."
    )]
    async fn search(
        &self,
        params: Parameters<SearchInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("search called: {}", input.query);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "search".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&result, 0),
        )]))
    }

    /// Create a new issue in the backlog.
    /// Use this to record discoveries or related work that should be tracked separately.
    #[tool(description = "Create a new issue in the backlog")]
    async fn create_issue(
        &self,
        params: Parameters<CreateIssueInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("create_issue called: {}", input.title);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "create_issue".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Update an existing issue's title and/or description.
    #[tool(description = "Update an existing issue")]
    async fn update_issue(
        &self,
        params: Parameters<UpdateIssueInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("update_issue called: {}", input.issue_number);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "update_issue".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Execute a bash command.
    /// Quick commands stream output in the transcript.
    /// Use run_in_background=true for long-running commands (dev servers, watchers) to spawn a terminal tab.
    #[tool(
        description = "Execute bash commands. Use run_in_background=true for long-running commands like dev servers."
    )]
    async fn bash(&self, params: Parameters<BashInput>) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("bash") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        let bg = input.run_in_background.unwrap_or(false);
        let redacted = redact_command(&input.command);
        tracing::info!(
            "bash called: {} (bg={})",
            &redacted[..redacted.len().min(100)],
            bg
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "bash".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&result, 0),
        )]))
    }

    /// Read output from a background terminal session.
    /// Use this to check on long-running commands started with run_in_background=true.
    /// Kill a background terminal session.
    /// Takes the session_id from a previously created background bash session.
    #[tool(
        description = "Kill a background terminal. Takes a terminal slug (e.g., 'dev-server') or URI from bash response."
    )]
    async fn kill_shell(
        &self,
        params: Parameters<KillShellInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("kill_shell") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("kill_shell called: terminal={}", &input.terminal);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "kill_shell".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Launch a sub-agent to work on a delegated task.
    /// Use this to break down complex work by having specialized agents handle subtasks.
    /// The agent will run with its configured tools, prompt, and model settings.
    #[tool(description = "Launch a sub-agent to work on a delegated task")]
    async fn task(&self, params: Parameters<TaskInput>) -> Result<CallToolResult, rmcp::ErrorData> {
        let request_id = uuid::Uuid::new_v4();
        tracing::info!(
            "[DEBUG-TASK] task handler ENTERED request_id={}",
            request_id
        );

        if let Err(msg) = self.require_internal("task") {
            tracing::info!(
                "[DEBUG-TASK] task handler REJECTED (external mode) request_id={}",
                request_id
            );
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!(
            "[DEBUG-TASK] task called: request_id={} subagent_type={}, description={}, prompt_len={}, session={:?}",
            request_id,
            &input.subagent_type,
            &input.description,
            input.prompt.len(),
            input.session
        );

        // Single task calls don't have a parent tool_use_id
        tracing::info!(
            "[DEBUG-TASK] spawning single task request_id={}",
            request_id
        );
        let (result, _artifact_uri) = self.spawn_single_task(&input, None).await;
        tracing::info!(
            "[DEBUG-TASK] task handler COMPLETED request_id={}",
            request_id
        );
        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&result, 0),
        )]))
    }

    /// Launch multiple sub-agents in parallel and wait for all to complete.
    /// When spawning agents that modify code (Build, QuickBuild): each task must have
    /// clearly separated, non-overlapping file responsibilities — if two agents might
    /// touch the same file or area, run them sequentially instead. Task prompts should
    /// instruct sub-agents to skip build and test steps; the parent handles build
    /// verification after all complete.
    #[tool(
        description = "Launch multiple sub-agents in parallel and wait for all to complete. When spawning agents that modify code (Build, QuickBuild): each task must have clearly separated, non-overlapping file responsibilities — if two agents might touch the same file or area, run them sequentially instead. Task prompts should instruct sub-agents to skip build and test steps; the parent handles build verification after all complete."
    )]
    async fn batch_tasks(
        &self,
        params: Parameters<BatchTasksInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let request_id = uuid::Uuid::new_v4();
        tracing::info!(
            "[DEBUG-BATCH] batch_tasks handler ENTERED request_id={}",
            request_id
        );

        if let Err(msg) = self.require_internal("batch_tasks") {
            tracing::info!(
                "[DEBUG-BATCH] batch_tasks handler REJECTED (external mode) request_id={}",
                request_id
            );
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }

        let input = params.0;
        if input.tasks.is_empty() {
            tracing::info!(
                "[DEBUG-BATCH] batch_tasks handler REJECTED (no tasks) request_id={}",
                request_id
            );
            return Ok(CallToolResult::success(vec![Content::text(
                "No tasks provided",
            )]));
        }

        tracing::info!(
            "[DEBUG-BATCH] batch_tasks called with {} tasks, request_id={}",
            input.tasks.len(),
            request_id
        );

        // Generate a unique batch ID to link all child jobs
        let batch_id = uuid::Uuid::new_v4().to_string();
        tracing::info!(
            "[DEBUG-BATCH] batch_tasks batch_id={} request_id={}",
            batch_id,
            request_id
        );

        // Log each task being spawned
        for (i, task) in input.tasks.iter().enumerate() {
            tracing::info!(
                "[DEBUG-BATCH] Task {}: subagent_type={}, description={}, request_id={}, batch_id={}",
                i + 1,
                &task.subagent_type,
                &task.description,
                request_id,
                batch_id
            );
        }

        // Clone tasks and add task_index to each
        let tasks_with_index: Vec<TaskInput> = input
            .tasks
            .iter()
            .enumerate()
            .map(|(i, task)| {
                let mut task_clone = task.clone();
                task_clone.task_index = Some(i as i32);
                task_clone
            })
            .collect();

        // Spawn all tasks concurrently with the batch_id as parent
        tracing::info!(
            "[DEBUG-BATCH] About to spawn {} tasks, request_id={}, batch_id={}",
            tasks_with_index.len(),
            request_id,
            batch_id
        );
        let futures: Vec<_> = tasks_with_index
            .iter()
            .enumerate()
            .map(|(i, task)| {
                tracing::info!(
                    "[DEBUG-BATCH] Creating future for task {} ({}), request_id={}, batch_id={}",
                    i,
                    task.subagent_type,
                    request_id,
                    batch_id
                );
                self.spawn_single_task(task, Some(&batch_id))
            })
            .collect();
        tracing::info!(
            "[DEBUG-BATCH] Created {} futures, request_id={}, batch_id={}",
            futures.len(),
            request_id,
            batch_id
        );

        // Wait for all to complete
        tracing::info!(
            "[DEBUG-BATCH] Waiting for all futures to complete, request_id={}, batch_id={}",
            request_id,
            batch_id
        );
        let results = futures::future::join_all(futures).await;
        tracing::info!(
            "[DEBUG-BATCH] All futures completed, got {} results, request_id={}, batch_id={}",
            results.len(),
            request_id,
            batch_id
        );

        // Collect results and artifact URIs separately
        let (result_texts, artifact_uris): (Vec<String>, Vec<Option<String>>) =
            results.into_iter().unzip();

        // Format combined output
        let output = result_texts
            .iter()
            .enumerate()
            .map(|(i, result)| {
                format!(
                    "## Task {}: {}\n\n{}",
                    i + 1,
                    input.tasks[i].description,
                    result
                )
            })
            .collect::<Vec<_>>()
            .join("\n\n---\n\n");

        // If output is too large, point the agent to individual artifact URIs instead
        if output.len() > MAX_RESULT_CHARS {
            let uri_lines: Vec<String> = artifact_uris
                .iter()
                .enumerate()
                .filter_map(|(i, uri)| {
                    uri.as_ref()
                        .map(|u| format!("- {} ({})", u, input.tasks[i].description))
                })
                .collect();

            if !uri_lines.is_empty() {
                let truncation_msg = format!(
                    "Output too large ({} chars). Read individual task results:\n{}",
                    output.len(),
                    uri_lines.join("\n")
                );
                return Ok(CallToolResult::success(vec![Content::text(truncation_msg)]));
            }
        }

        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&output, 0),
        )]))
    }

    /// Retrieve a skill's instructions by ID or name.
    /// Use this to access skill content on-demand when needed for a task.
    #[tool(description = "Retrieve a skill's instructions by ID or name")]
    async fn skill(
        &self,
        params: Parameters<SkillInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("skill") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("skill called: {}", input.skill);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "skill".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&result, 0),
        )]))
    }

    /// List all available skills with metadata.
    /// Returns skill names, descriptions, scope, and supporting file info.
    #[tool(
        description = "List all skills with metadata. Filter by scope: workspace, project, or all (default)."
    )]
    async fn list_skills(
        &self,
        params: Parameters<ListSkillsInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("list_skills") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("list_skills called (scope={:?})", input.scope);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "list_skills".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&result, 0),
        )]))
    }

    /// Create a new skill in spec-compliant directory format.
    /// Name must be a valid slug (lowercase letters, digits, hyphens, 1-64 chars).
    #[tool(
        description = "Create a new skill with spec-compliant directory format. Name must be lowercase slug (letters, digits, hyphens)."
    )]
    async fn create_skill(
        &self,
        params: Parameters<CreateSkillInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("create_skill") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("create_skill called: {}", input.name);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "create_skill".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Update an existing skill's description, prompt, tools, or model.
    /// Supports full prompt replacement, append, or section replacement by heading.
    #[tool(
        description = "Update a skill's description, prompt, allowed tools, or model. Supports prompt replacement, append, or section replacement."
    )]
    async fn update_skill(
        &self,
        params: Parameters<UpdateSkillInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("update_skill") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("update_skill called: {}", input.id);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "update_skill".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Delete a skill by ID.
    #[tool(description = "Delete a skill by ID")]
    async fn delete_skill(
        &self,
        params: Parameters<DeleteSkillInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("delete_skill") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("delete_skill called: {}", input.id);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "delete_skill".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// List active memories for the current project and workspace-global memories.
    #[tool(description = "List active memories for the current project")]
    async fn list_memories(
        &self,
        params: Parameters<ListMemoriesInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("list_memories called (active_only={})", input.active_only);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "list_memories".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Create a new memory for contextual surfacing.
    /// Memories surface as contextual footnotes when agents use tools matching the triggers or keywords.
    #[tool(description = "Create a memory with trigger conditions for contextual surfacing")]
    async fn create_memory(
        &self,
        params: Parameters<CreateMemoryInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        let triggers = input.triggers.unwrap_or_default();
        tracing::info!(
            "create_memory called: {} ({} triggers)",
            &input.content[..input.content.len().min(50)],
            triggers.len()
        );

        // Rebuild payload with unwrapped triggers for the handler
        let payload = serde_json::json!({
            "content": input.content,
            "confidence": input.confidence,
            "sourceIssue": input.source_issue,
            "triggers": triggers,
            "scope": input.scope,
            "keywords": input.keywords,
            "sourceRunId": input.source_run_id,
        });

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "create_memory".to_string(),
            payload,
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Update an existing memory's content, confidence, active status, or triggers.
    #[tool(
        description = "Update a memory's content, confidence, active status, or triggers. When triggers are provided, they replace all existing triggers."
    )]
    async fn update_memory(
        &self,
        params: Parameters<UpdateMemoryInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("update_memory called: {}", input.id);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "update_memory".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Deactivate a memory so it no longer surfaces.
    #[tool(description = "Deactivate a memory so it stops surfacing")]
    async fn deactivate_memory(
        &self,
        params: Parameters<DeactivateMemoryInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("deactivate_memory called: {}", input.id);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "deactivate_memory".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Report a bug or friction with Cairn tools, prompts, or harness.
    /// For Cairn-internal issues only — not user code problems.
    /// Reports are sent to the Cairn team for review.
    /// Code snippets and file paths are fine for clarity.
    /// Do NOT include personally identifiable information (usernames, emails, API keys).
    #[tool(
        description = "Report a bug or friction with Cairn tools, prompts, or harness.\nFor Cairn-internal issues only — not user code problems.\nReports are sent to the Cairn team for review.\nCode snippets and file paths are fine for clarity.\nDo NOT include personally identifiable information (usernames, emails, API keys)."
    )]
    async fn bug_report(
        &self,
        params: Parameters<BugReportInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("bug_report") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }

        // Rate limit: max 3 per session
        let count = self.report_count.fetch_add(1, Ordering::Relaxed);
        if count >= 3 {
            self.report_count.fetch_sub(1, Ordering::Relaxed);
            return Ok(CallToolResult::success(vec![Content::text(
                "Rate limit reached: maximum 3 bug reports per session.",
            )]));
        }

        let input = params.0;
        tracing::info!("bug_report called: {} - {}", input.category, input.title);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "bug_report".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Execute JavaScript/TypeScript code in a Bun runtime with access to MCP tools.
    /// Code is wrapped in an async function — use `return` to produce output.
    /// Falls back to stdout (console.log) if no return value.
    #[tool(
        description = "Execute JavaScript/TypeScript code in a Bun runtime with access to MCP tools.\n\nCode runs inside an async function. Use `return` to produce output (string or object).\nFalls back to stdout (console.log) if no explicit return.\n\nIMPORTANT: The runtime inherits YOUR current tool permissions. Only tools you are allowed to use will exist on the `mcp` object. Calling a tool you don't have permission for will throw a TypeError.\n\nThe runtime provides an `mcp` object with these tools (if permitted):\n- mcp.read({path}) - Read files or cairn:// URIs\n- mcp.write({file_path, content, commit_msg}) - Write files\n- mcp.edit({file_path, old_string, new_string, commit_msg}) - Edit files\n- mcp.bash({command}) - Run shell commands\n- mcp.search({query}) - Search issues/comments\n- mcp.add_comment({content}) - Comment on current issue\n- mcp.create_issue({title, description?}) - Create issues\n- mcp.update_issue({issueNumber, title?, description?}) - Update issues\n- mcp.task({description, prompt, subagentType}) - Spawn sub-agents\n\nAll mcp tools return Promise<{content: [{type, text}], isError?}>.\nTop-level `read`, `write`, `edit`, `bash` are also available as shortcuts.\n\nConstants: CWD (working directory), RUN_ID, PROJECT_ID"
    )]
    async fn execute(
        &self,
        params: Parameters<ExecuteInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("execute") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!("execute called with {} chars of code", input.code.len());

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "execute".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(parse_tool_response(&result))
    }

    /// Handle permission prompts from Claude CLI.
    /// This tool is called by Claude when using --permission-prompt-tool flag.
    /// Blocks until the user approves or denies the tool execution.
    #[tool(description = "Handle permission requests from Claude CLI")]
    async fn permission_prompt(
        &self,
        params: Parameters<PermissionPromptInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;

        tracing::info!(
            "[PERMISSION-DEBUG] permission_prompt called: tool={}, tool_use_id={:?}",
            input.tool_name,
            input.tool_use_id
        );

        if let Err(msg) = self.require_internal("permission_prompt") {
            return Ok(CallToolResult::success(vec![Content::text(
                serde_json::json!({"behavior": "deny", "message": msg}).to_string(),
            )]));
        }

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "permission_prompt".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        // Call Tauri and wait for response (blocking until user responds)
        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Fast file pattern matching tool that works with any codebase size.
    /// Supports glob patterns like "**/*.js" or "src/**/*.ts".
    /// Returns matching file paths sorted by modification time (most recent first).
    /// Respects .gitignore rules.
    #[tool(
        description = "Fast file pattern matching tool that works with any codebase size.\nSupports glob patterns like \"**/*.js\" or \"src/**/*.ts\".\nReturns matching file paths sorted by modification time (most recent first).\nRespects .gitignore rules."
    )]
    async fn glob(&self, params: Parameters<GlobInput>) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("glob") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!(
            "glob called: pattern={}, path={:?}",
            input.pattern,
            input.path
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "glob".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&result, 0),
        )]))
    }

    /// Search file contents using ripgrep. Supports regex patterns, file type filtering,
    /// context lines, and multiple output modes.
    #[tool(
        description = "Search file contents using ripgrep (rg). Prefer this over running `rg` through bash: it already uses `rg` under the hood and applies Cairn path scoping automatically.\n\nSupports:\n- Full regex syntax (e.g., \"log.*Error\", \"function\\s+\\w+\")\n- File filtering by glob pattern or type\n- Output modes: \"files_with_matches\" (default), \"content\", \"count\"\n- Context lines around matches (-A, -B, -C)\n- Case insensitive search (-i)\n- Multiline matching\n- Result pagination (head_limit, offset)"
    )]
    async fn grep(&self, params: Parameters<GrepInput>) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("grep") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!(
            "grep called: pattern={}, path={:?}",
            input.pattern,
            input.path
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "grep".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(
            cap_text_result(&result, 0),
        )]))
    }
    /// Fetch content from a URL, convert HTML to markdown, and optionally interpret with an LLM.
    /// Returns processed content relevant to the prompt.
    #[tool(
        description = "Fetch a web page, convert to markdown, and extract information relevant to a prompt. Returns processed content with smart truncation."
    )]
    async fn web_fetch(
        &self,
        params: Parameters<WebFetchInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("web_fetch") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        let max_chars = input.max_length.unwrap_or(MAX_RESULT_CHARS);
        let raw = input.raw.unwrap_or(false);
        tracing::info!(
            "web_fetch called: url={}, prompt_len={}",
            input.url,
            input.prompt.len()
        );

        // Normalize URL
        let url = match normalize_url(&input.url) {
            Ok(u) => u,
            Err(e) => return Ok(CallToolResult::error(vec![Content::text(e)])),
        };
        let domain = extract_domain(&url);
        let prompt_hash = hash_prompt(&input.prompt);

        // Check cache
        if let Ok(mut cache) = self.web_cache.lock() {
            if let Some(cached) = cache.get(url.as_str(), prompt_hash) {
                tracing::info!("web_fetch cache hit for {}", url);
                return Ok(CallToolResult::success(vec![Content::text(
                    cached.to_string(),
                )]));
            }
        }

        // Check rate limit
        if let Ok(mut limiter) = self.web_rate_limiter.lock() {
            if let Err(wait_secs) = limiter.check(&domain) {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Rate limit: too many requests to {}. Try again in {}s.",
                    domain, wait_secs
                ))]));
            }
        }

        // Fetch the URL
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::limited(10))
            .user_agent("Mozilla/5.0 (compatible; CairnBot/1.0)")
            .build()
            .unwrap_or_default();

        let response = match client.get(url.as_str()).send().await {
            Ok(r) => r,
            Err(e) => {
                let msg = if e.is_timeout() {
                    format!("Request timed out fetching {}. Try again.", url)
                } else {
                    format!("Failed to fetch {}: {}", url, e)
                };
                return Ok(CallToolResult::error(vec![Content::text(msg)]));
            }
        };

        // Check for redirect to different host
        let final_url = response.url().clone();
        if final_url.host_str() != url.host_str() {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Redirected to a different host: {}\nMake a new request with this URL.",
                final_url
            ))]));
        }

        let status = response.status();
        if !status.is_success() {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "HTTP {} fetching {}",
                status.as_u16(),
                url
            ))]));
        }

        // Check content type
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let body = match response.text().await {
            Ok(t) => t,
            Err(e) => {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to read response body: {}",
                    e
                ))]));
            }
        };

        // For non-HTML content, return raw text truncated
        let is_html = content_type.contains("html") || body.trim_start().starts_with('<');
        let result = if is_html {
            let title = extract_title(&body);
            let markdown = html_to_markdown(&body);
            let markdown_len = markdown.len();

            let processed = if raw {
                if markdown.len() > max_chars {
                    markdown[..max_chars].to_string()
                } else {
                    markdown
                }
            } else {
                smart_truncate(&markdown, &input.prompt, max_chars)
            };

            // Format with title and URL prefix
            let mut output = String::new();
            if let Some(t) = title {
                output.push_str(&format!("# {}\n", t));
            }
            output.push_str(&format!("URL: {}\n\n", final_url));
            output.push_str(&processed);

            // Try LLM interpretation for large content
            if !raw && markdown_len > 20_000 {
                let interpret_request = CallbackRequest {
                    cwd: self.cwd.to_string(),
                    run_id: self.run_id.as_ref().map(|r| r.to_string()),
                    tool: "interpret_content".to_string(),
                    payload: serde_json::json!({
                        "url": final_url.to_string(),
                        "prompt": input.prompt,
                        "content": &processed[..processed.len().min(80_000)],
                    }),
                    tool_use_id: None,
                };

                let interpreted = self.call_tauri(&interpret_request).await;
                // If the callback returned meaningful content (not an error), use it
                if !interpreted.is_empty()
                    && !interpreted.starts_with("Error")
                    && !interpreted.starts_with("Unknown tool")
                    && interpreted.len() > 50
                {
                    let mut llm_output = String::new();
                    if let Some(t) = extract_title(&body) {
                        llm_output.push_str(&format!("# {}\n", t));
                    }
                    llm_output.push_str(&format!("URL: {}\n\n", final_url));
                    llm_output.push_str(&interpreted);
                    // Cache the LLM result
                    if let Ok(mut cache) = self.web_cache.lock() {
                        cache.put(url.as_str(), prompt_hash, llm_output.clone());
                    }
                    return Ok(CallToolResult::success(vec![Content::text(llm_output)]));
                }
            }

            output
        } else {
            // Non-HTML: return truncated raw text
            let truncated = if body.len() > max_chars {
                body[..max_chars].to_string()
            } else {
                body
            };
            format!(
                "URL: {}\nContent-Type: {}\n\n{}",
                final_url, content_type, truncated
            )
        };

        // Cache result
        if let Ok(mut cache) = self.web_cache.lock() {
            cache.put(url.as_str(), prompt_hash, result.clone());
        }

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Search the web using a search provider API.
    /// Returns structured search results with titles, URLs, and snippets.
    #[tool(
        description = "Search the web and return structured results with titles, URLs, and snippets. Requires a search API key configured in settings."
    )]
    async fn web_search(
        &self,
        params: Parameters<WebSearchInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("web_search") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        let limit = input.limit.unwrap_or(10).min(20);
        tracing::info!("web_search called: query={}", input.query);

        // Get API key from environment
        let api_key = match env::var("WEB_SEARCH_API_KEY") {
            Ok(key) if !key.is_empty() => key,
            _ => {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Web search requires a search API key. Get a free Jina API key at https://jina.ai/api-dashboard/ and configure it as webSearchApiKey in ~/.cairn/settings.yaml"
                )]));
            }
        };

        // Check cache
        let cache_key_prompt = format!("search:{}", input.query);
        let prompt_hash = hash_prompt(&cache_key_prompt);
        if let Ok(mut cache) = self.web_cache.lock() {
            if let Some(cached) = cache.get("__web_search__", prompt_hash) {
                tracing::info!("web_search cache hit for query={}", input.query);
                return Ok(CallToolResult::success(vec![Content::text(
                    cached.to_string(),
                )]));
            }
        }

        // Call Jina Search API
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        let search_url = format!("https://s.jina.ai/{}", urlencoding::encode(&input.query));

        let response = match client
            .get(&search_url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Accept", "application/json")
            .header("X-Retain-Images", "none")
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Search request failed: {}",
                    e
                ))]));
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Search API returned HTTP {}: {}",
                status.as_u16(),
                &body[..body.len().min(200)]
            ))]));
        }

        let body: serde_json::Value = match response.json().await {
            Ok(v) => v,
            Err(e) => {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to parse search results: {}",
                    e
                ))]));
            }
        };

        // Parse Jina results
        let results = body.get("data").and_then(|d| d.as_array());
        let results = match results {
            Some(r) => r,
            None => {
                return Ok(CallToolResult::success(vec![Content::text(
                    "No search results found.",
                )]));
            }
        };

        // Format results with domain filtering
        let mut output = String::new();
        let mut count = 0;

        for item in results {
            if count >= limit {
                break;
            }

            let title = item.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let url = item.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let description = item
                .get("description")
                .and_then(|v| v.as_str())
                .or_else(|| item.get("content").and_then(|v| v.as_str()))
                .unwrap_or("");

            // Apply domain filters
            if let Ok(parsed_url) = url::Url::parse(url) {
                let domain = parsed_url.host_str().unwrap_or("");

                if let Some(ref allowed) = input.allowed_domains {
                    if !allowed.iter().any(|d| domain.contains(d.as_str())) {
                        continue;
                    }
                }

                if let Some(ref blocked) = input.blocked_domains {
                    if blocked.iter().any(|d| domain.contains(d.as_str())) {
                        continue;
                    }
                }
            }

            count += 1;
            output.push_str(&format!("{}. {}\n", count, title));
            output.push_str(&format!("   URL: {}\n", url));
            // Truncate long descriptions
            let desc = if description.len() > 300 {
                format!("{}...", &description[..300])
            } else {
                description.to_string()
            };
            output.push_str(&format!("   {}\n\n", desc));
        }

        if output.is_empty() {
            output = "No search results found.".to_string();
        } else {
            output = format!("Search results for \"{}\":\n\n{}", input.query, output);
        }

        // Cache results
        if let Ok(mut cache) = self.web_cache.lock() {
            cache.put("__web_search__", prompt_hash, output.clone());
        }

        Ok(CallToolResult::success(vec![Content::text(output)]))
    }
}

impl CairnMcp {
    /// Spawn a single task agent and wait for its result.
    /// Used by both `task` and `batch_tasks` handlers.
    ///
    /// Returns `(result_text, artifact_uri)` where `artifact_uri` is a cairn:// URI
    /// for the task's artifact (set when the task completed successfully).
    ///
    /// # Arguments
    /// * `input` - The task input parameters
    /// * `parent_tool_use_id` - For batch_tasks: the parent's tool_use_id to link child jobs
    async fn spawn_single_task(
        &self,
        input: &TaskInput,
        parent_tool_use_id: Option<&str>,
    ) -> (String, Option<String>) {
        let spawn_id = uuid::Uuid::new_v4();
        tracing::info!("[DEBUG-SPAWN] spawn_single_task ENTERED spawn_id={} subagent_type={} parent_tool_use_id={:?}", 
            spawn_id, input.subagent_type, parent_tool_use_id);

        // Log warnings for unsupported features
        if input.run_in_background.unwrap_or(false) {
            tracing::warn!("run_in_background is not yet supported for Cairn task tool");
        }
        let payload = serde_json::to_value(input).unwrap_or_default();
        tracing::info!(
            "[DEBUG-SPAWN] task callback payload spawn_id={} session={} payload={}",
            spawn_id,
            input.session.as_deref().unwrap_or("new"),
            payload
        );
        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "task".to_string(),
            payload,
            tool_use_id: parent_tool_use_id.map(|s| s.to_string()),
        };

        tracing::info!(
            "[DEBUG-SPAWN] Calling Tauri callback spawn_id={} subagent_type={}",
            spawn_id,
            input.subagent_type
        );
        let response = self.call_tauri_full(&request).await;
        tracing::info!(
            "[DEBUG-SPAWN] spawn_single_task COMPLETED spawn_id={} subagent_type={}",
            spawn_id,
            input.subagent_type
        );
        (response.result, response.artifact_uri)
    }

    /// Like `call_tauri` but returns the full `CallbackResponse` (including `artifact_uri`).
    async fn call_tauri_full(&self, request: &CallbackRequest) -> CallbackResponse {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(MCP_CALLBACK_TIMEOUT_SECS))
            .build()
        {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("Failed to build HTTP client: {}", e);
                return CallbackResponse {
                    result: format!("Error building HTTP client: {}", e),
                    artifact_uri: None,
                };
            }
        };
        let mut req = client.post(self.callback_url.as_str()).json(request);

        if let Some(secret) = &self.mcp_secret {
            req = req.header("Authorization", format!("Bearer {}", secret));
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                match resp.text().await {
                    Ok(text) => match serde_json::from_str::<CallbackResponse>(&text) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(
                                "Failed to parse response (status {}): {} - body: {}",
                                status,
                                e,
                                &text[..text.len().min(500)]
                            );
                            CallbackResponse {
                                result: format!(
                                    "Error parsing response: {} (body: {})",
                                    e,
                                    &text[..text.len().min(200)]
                                ),
                                artifact_uri: None,
                            }
                        }
                    },
                    Err(e) => CallbackResponse {
                        result: format!("Error reading response body: {}", e),
                        artifact_uri: None,
                    },
                }
            }
            Err(e) => CallbackResponse {
                result: format!("Error calling Tauri: {}", e),
                artifact_uri: None,
            },
        }
    }

    /// Handle the dynamic return tool - submit final output and complete the job
    async fn handle_return(
        &self,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        // Check if we have a schema (shouldn't be called without one, but be safe)
        if self.artifact_schema.is_none() {
            return Ok(CallToolResult::success(vec![Content::text(
                "return is not available - no output schema was provided for this job",
            )]));
        }

        let payload = arguments.unwrap_or_default();
        tracing::info!("return called with {} fields", payload.len());

        // Forward to Tauri callback - marks job complete and advances DAG
        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "return".to_string(),
            payload: serde_json::Value::Object(payload),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    async fn call_tauri(&self, request: &CallbackRequest) -> String {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(MCP_CALLBACK_TIMEOUT_SECS))
            .build()
        {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("Failed to build HTTP client: {}", e);
                return format!("Error building HTTP client: {}", e);
            }
        };
        let mut req = client.post(self.callback_url.as_str()).json(request);

        // Authenticate with shared secret as static bearer token
        if let Some(secret) = &self.mcp_secret {
            req = req.header("Authorization", format!("Bearer {}", secret));
        }

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                match resp.text().await {
                    Ok(text) => {
                        // Try to parse as JSON
                        match serde_json::from_str::<CallbackResponse>(&text) {
                            Ok(r) => r.result,
                            Err(e) => {
                                tracing::error!(
                                    "Failed to parse response (status {}): {} - body: {}",
                                    status,
                                    e,
                                    &text[..text.len().min(500)]
                                );
                                format!(
                                    "Error parsing response: {} (body: {})",
                                    e,
                                    &text[..text.len().min(200)]
                                )
                            }
                        }
                    }
                    Err(e) => format!("Error reading response body: {}", e),
                }
            }
            Err(e) => format!("Error calling Tauri: {}", e),
        }
    }
}

impl ServerHandler for CairnMcp {
    fn get_info(&self) -> ServerInfo {
        let mut instructions = if self.external_mode {
            "Cairn MCP server (external mode).\n\n\
             Issue tools:\n\
             - read_uri: Read Cairn resources (cairn://PROJECT/NUMBER for issues, cairn://PROJECT/NUMBER/EXEC/NODE/chat for transcripts)\n\
             - search: Search issues, comments, and past conversations\n\
             - create_issue: Create a new issue in the backlog\n\
             - update_issue: Update an existing issue\n\
             - add_comment: Record discoveries, status updates, or notes on issues"
                .to_string()
        } else {
            "Cairn MCP server for agent orchestration.\n\n\
             Communication tools:\n\
             - ask_user: Ask the user for clarification\n\
             - add_comment: Record discoveries, status updates, or notes on the issue\n\n\
             Issue tools:\n\
             - read: Read issue/node resources (cairn://PROJECT/NUMBER, cairn://PROJECT/NUMBER/NODE)\n\
             - create_issue: Create a new issue in the backlog\n\
             - update_issue: Update an existing issue\n\n\
             Implementation tools:\n\
             - read: Read file contents, directory listings, or cairn:// resources\n\
             - edit: Apply file changes and commit (add, update, delete)\n\
             - bash: Execute shell commands (use run_in_background=true and terminal=\"name\" for dev servers, returns resource URI)\n\
             - kill_shell: Kill a background terminal by slug or URI\n\
             - task: Launch a sub-agent to work on a delegated task\n\
             - batch_tasks: Launch multiple sub-agents in parallel and wait for all to complete"
                .to_string()
        };

        // Add available agents to instructions
        if !self.available_agents.is_empty() {
            instructions.push_str("\n\nAvailable agents for task tool:\n");
            for agent in &self.available_agents {
                instructions.push_str(&format!("- {}: {}\n", agent.name, agent.description));
            }
        }

        // Add available skills to instructions
        if !self.available_skills.is_empty() {
            instructions.push_str("\n\nAvailable skills (use skill tool to retrieve):\n");
            for skill in &self.available_skills {
                instructions.push_str(&skill.format_list_item());
                instructions.push('\n');
            }
        }

        // Add output tool to instructions if schema is provided
        if self.artifact_schema.is_some() {
            instructions.push_str(&format!(
                "\n\nOutput tools:\n\
                 - {}: Submit final output and complete the job",
                self.output_tool_name
            ));
        }

        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_resources_list_changed()
                .build(),
            server_info: Implementation {
                name: "cairn-mcp".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            instructions: Some(instructions),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            // Get static tools from the router
            let mut tools = self.tool_router.list_all();

            // read_uri is external-only; internal agents use 'read' instead
            if !self.external_mode {
                tools.retain(|t| t.name != "read_uri");
            }

            // Modify task tool description to include available agents
            if !self.available_agents.is_empty() {
                for tool in &mut tools {
                    if tool.name == "task" {
                        let mut desc =
                            "Launch a sub-agent to work on a delegated task.\n\nAvailable agents:\n"
                                .to_string();
                        for agent in &self.available_agents {
                            desc.push_str(&format!("- {}: {}\n", agent.name, agent.description));
                        }
                        tool.description = Some(std::borrow::Cow::Owned(desc));
                        break;
                    }
                }
            }

            // Modify skill tool description to include available skills
            if !self.available_skills.is_empty() {
                for tool in &mut tools {
                    if tool.name == "skill" {
                        let mut desc =
                            "Retrieve a skill's instructions.\n\nAvailable skills:\n".to_string();
                        for skill in &self.available_skills {
                            desc.push_str(&skill.format_list_item());
                            desc.push('\n');
                        }
                        tool.description = Some(std::borrow::Cow::Owned(desc));
                        break;
                    }
                }
            }

            // Add dynamic output tool if schema is provided
            if let Some(schema) = &self.artifact_schema {
                let description = self.output_tool_description.clone().unwrap_or_else(|| {
                    format!(
                        "Submit output via {}. The schema defines the required fields.",
                        self.output_tool_name
                    )
                });
                let output_tool = Tool::new(
                    self.output_tool_name.clone(),
                    description,
                    schema.as_ref().clone(),
                );
                tools.push(output_tool);
            }

            // Add custom tools
            let static_tool_names: std::collections::HashSet<String> =
                tools.iter().map(|t| t.name.to_string()).collect();
            for custom_tool in &self.available_tools {
                // Skip if name collides with a static tool
                if static_tool_names.contains(custom_tool.id.as_str()) {
                    tracing::warn!(
                        "Custom tool '{}' skipped: name collides with built-in tool",
                        custom_tool.id
                    );
                    continue;
                }
                if let Some(schema_obj) = custom_tool.input_schema.as_object() {
                    let tool = Tool::new(
                        custom_tool.id.clone(),
                        custom_tool.description.clone(),
                        schema_obj.clone(),
                    );
                    tools.push(tool);
                }
            }

            Ok(ListToolsResult::with_all_items(tools))
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParam,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            let tool_name = request.name.as_ref();

            // Handle output tool specially (dynamic schema-based output)
            if tool_name == self.output_tool_name {
                return self.handle_return(request.arguments).await;
            }

            // Check if it's a custom tool
            if self.available_tools.iter().any(|t| t.id == tool_name) {
                let callback_request = CallbackRequest {
                    cwd: self.cwd.to_string(),
                    run_id: self.run_id.as_ref().map(|r| r.to_string()),
                    tool: "custom_tool".to_string(),
                    payload: serde_json::json!({
                        "tool_id": tool_name,
                        "inputs": request.arguments.unwrap_or_default()
                    }),
                    tool_use_id: None,
                };
                let result = self.call_tauri(&callback_request).await;
                return Ok(parse_tool_response(&result));
            }

            // Delegate to router for static tools
            let tcc = ToolCallContext::new(self, request, context);
            self.tool_router.call(tcc).await
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourcesResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            tracing::info!(
                "list_resources called, external_mode={}",
                self.external_mode
            );

            if self.external_mode {
                tracing::info!("list_resources: returning empty (external mode)");
                return Ok(ListResourcesResult::default());
            }

            // Call Tauri callback to get terminal list
            let request = CallbackRequest {
                cwd: self.cwd.to_string(),
                run_id: self.run_id.as_ref().map(|r| r.to_string()),
                tool: "list_resources".to_string(),
                payload: serde_json::json!({}),
                tool_use_id: None,
            };

            let result = self.call_tauri(&request).await;

            // Parse the response as a list of terminal resources
            match serde_json::from_str::<Vec<TerminalResourceInfo>>(&result) {
                Ok(terminals) => {
                    let resources = terminals
                        .into_iter()
                        .map(|t| {
                            Annotated::new(
                                RawResource {
                                    uri: t.uri,
                                    name: t.name,
                                    description: t.description,
                                    mime_type: Some("text/plain".to_string()),
                                    size: None,
                                },
                                None,
                            )
                        })
                        .collect();
                    Ok(ListResourcesResult::with_all_items(resources))
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to parse terminal resources: {} - response: {}",
                        e,
                        result
                    );
                    Ok(ListResourcesResult::default())
                }
            }
        }
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ReadResourceResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            if self.external_mode {
                return Err(rmcp::ErrorData::invalid_request(
                    "Resources not available in external mode",
                    None,
                ));
            }

            let uri = &request.uri;
            tracing::info!("read_resource called: uri={}", uri);

            // Determine which callback to use based on URI scheme
            let tool_name = if uri.starts_with("cairn://") {
                // Parse cairn:// URI to determine resource type
                match parse_cairn_uri(uri) {
                    Some(resource) => {
                        match resource {
                            // Terminal resources use read_resource
                            CairnResource::NodeTerminal { .. }
                            | CairnResource::ProjectTerminal { .. } => "read_resource",
                            // All other resources use read_issue_resource
                            _ => "read_issue_resource",
                        }
                    }
                    None => {
                        return Err(rmcp::ErrorData::invalid_request(
                            format!("Invalid cairn resource URI: {}", uri),
                            None,
                        ));
                    }
                }
            } else {
                return Err(rmcp::ErrorData::invalid_request(
                    format!("Unknown resource scheme: {}", uri),
                    None,
                ));
            };

            // Call Tauri callback
            let callback_request = CallbackRequest {
                cwd: self.cwd.to_string(),
                run_id: self.run_id.as_ref().map(|r| r.to_string()),
                tool: tool_name.to_string(),
                payload: serde_json::json!({ "uri": uri }),
                tool_use_id: None,
            };

            let result = self.call_tauri(&callback_request).await;

            // For terminal resources, parse the structured response
            if tool_name == "read_resource" {
                match serde_json::from_str::<TerminalReadResult>(&result) {
                    Ok(terminal_result) => {
                        let contents = vec![ResourceContents::text(
                            cap_text_result(&terminal_result.output, 0),
                            uri,
                        )];
                        return Ok(ReadResourceResult { contents });
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to parse terminal read result: {} - response: {}",
                            e,
                            result
                        );
                    }
                }
            }

            // For issue resources (or fallback), return the result directly
            // The backend returns properly formatted content
            let contents = vec![ResourceContents::text(cap_text_result(&result, 0), uri)];
            Ok(ReadResourceResult { contents })
        }
    }
}

/// Terminal resource info returned from Tauri
#[derive(Debug, Clone, Deserialize)]
struct TerminalResourceInfo {
    uri: String,
    name: String,
    description: Option<String>,
}

/// Terminal read result returned from Tauri
#[derive(Debug, Clone, Deserialize)]
struct TerminalReadResult {
    output: String,
    status: String,
    #[serde(default)]
    exit_code: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize unified logging (file + stderr; stdout reserved for MCP protocol)
    let _log_guard = cairn_common::logging::init(cairn_common::logging::LogConfig {
        process: cairn_common::logging::ProcessTag::Mcp,
        log_dir: None,
        stderr: true,
    })
    .expect("Failed to initialize logging");

    // Callback URL - passed from main app via MCP config env var
    let callback_url = env::var("CAIRN_CALLBACK_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3847/api/mcp".to_string());
    // Get current working directory - fallback for run identification
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    // Run ID - preferred method for accurate run identification (avoids cwd ambiguity)
    let run_id = env::var("CAIRN_RUN_ID").ok();
    // Shared secret for bearer token authentication (base64-encoded)
    let mcp_secret = env::var("CAIRN_MCP_SECRET").ok();

    // Load artifact schema if provided
    let artifact_schema: Option<JsonSchemaObject> = if let Some(ref schema_path) = args.schema {
        match std::fs::read_to_string(schema_path) {
            Ok(content) => match serde_json::from_str::<serde_json::Value>(&content) {
                Ok(schema) => {
                    if let Some(obj) = schema.as_object() {
                        tracing::info!("Loaded artifact schema from: {}", schema_path);
                        Some(obj.clone())
                    } else {
                        tracing::error!("Schema file must contain a JSON object");
                        return Err(anyhow::anyhow!(
                            "Schema file must contain a JSON object, not {}",
                            match &schema {
                                serde_json::Value::Array(_) => "an array",
                                serde_json::Value::String(_) => "a string",
                                serde_json::Value::Number(_) => "a number",
                                serde_json::Value::Bool(_) => "a boolean",
                                serde_json::Value::Null => "null",
                                _ => "unknown type",
                            }
                        ));
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to parse schema JSON: {}", e);
                    return Err(anyhow::anyhow!("Failed to parse schema JSON: {}", e));
                }
            },
            Err(e) => {
                tracing::error!("Failed to read schema file: {}", e);
                return Err(anyhow::anyhow!("Failed to read schema file: {}", e));
            }
        }
    } else {
        None
    };

    tracing::info!("Starting cairn-mcp server");
    tracing::info!("Callback URL: {}", callback_url);
    tracing::info!("Working directory: {}", cwd);
    if let Some(ref id) = run_id {
        tracing::info!("Run ID: {}", id);
    }
    if args.external {
        tracing::info!("Running in external mode (limited toolset)");
    }
    if artifact_schema.is_some() {
        tracing::info!(
            "Output tool enabled: {} (with custom schema)",
            args.tool_name.as_deref().unwrap_or("return")
        );
    }

    // Parse available agents from JSON argument
    let available_agents: Vec<AgentInfo> = if let Some(ref agents_json) = args.agents {
        match serde_json::from_str::<Vec<AgentInfo>>(agents_json) {
            Ok(agents) => {
                tracing::info!("Loaded {} available agents", agents.len());
                agents
            }
            Err(e) => {
                tracing::warn!("Failed to parse agents JSON: {}", e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    // Parse available skills from JSON argument
    let available_skills: Vec<SkillInfo> = if let Some(ref skills_json) = args.skills {
        match serde_json::from_str::<Vec<SkillInfo>>(skills_json) {
            Ok(skills) => {
                tracing::info!("Loaded {} available skills", skills.len());
                skills
            }
            Err(e) => {
                tracing::warn!("Failed to parse skills JSON: {}", e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    // Parse available custom tools from JSON argument
    let available_tools: Vec<ToolInfo> = if let Some(ref tools_json) = args.tools {
        match serde_json::from_str::<Vec<ToolInfo>>(tools_json) {
            Ok(tools) => {
                tracing::info!("Loaded {} custom tools", tools.len());
                tools
            }
            Err(e) => {
                tracing::warn!("Failed to parse tools JSON: {}", e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let service = CairnMcp::new(
        callback_url,
        cwd,
        run_id,
        mcp_secret,
        args.external,
        artifact_schema,
        args.tool_name,
        args.tool_description,
        available_agents,
        available_skills,
        available_tools,
    );

    // Create stdio transport and run the server
    let transport = rmcp::transport::stdio();
    let server = service.serve(transport).await?;

    // Wait for the server to complete
    server.waiting().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> JsonSchemaObject {
        serde_json::from_str::<serde_json::Value>(
            r#"{
                "type": "object",
                "properties": {
                    "summary": { "type": "string", "description": "Brief summary" },
                    "confidence": { "type": "number", "minimum": 0, "maximum": 1 }
                },
                "required": ["summary"]
            }"#,
        )
        .unwrap()
        .as_object()
        .unwrap()
        .clone()
    }

    fn create_test_mcp_for_tools() -> CairnMcp {
        CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            false,
            None,
            None,
            None,
            vec![],
            vec![],
            vec![],
        )
    }

    fn static_tool(mcp: &CairnMcp, name: &str) -> Tool {
        mcp.tool_router
            .list_all()
            .into_iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("tool {name} should exist"))
    }

    #[test]
    fn test_cairn_mcp_without_schema_has_no_return_tool() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,   // No schema
            None,   // Default tool name
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        let tools = mcp.tool_router.list_all();
        let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();

        // return should NOT be in the static tool list
        assert!(
            !tool_names.contains(&"return"),
            "return should not be in static tools"
        );

        // Verify artifact_schema is None
        assert!(mcp.artifact_schema.is_none());
    }

    #[test]
    fn test_cairn_mcp_with_schema_stores_artifact_schema() {
        let schema = create_test_schema();
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            Some(schema.clone()),
            None,   // Default tool name
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        // Verify schema is stored
        assert!(mcp.artifact_schema.is_some());
        let stored_schema = mcp.artifact_schema.as_ref().unwrap();

        // Verify schema content
        assert!(stored_schema.contains_key("type"));
        assert!(stored_schema.contains_key("properties"));
        assert!(stored_schema.contains_key("required"));

        // Verify properties
        let properties = stored_schema
            .get("properties")
            .unwrap()
            .as_object()
            .unwrap();
        assert!(properties.contains_key("summary"));
        assert!(properties.contains_key("confidence"));
    }

    #[test]
    fn test_list_tools_includes_return_when_schema_provided() {
        let schema = create_test_schema();
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            Some(schema.clone()),
            None,   // Default tool name
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        // Get static tools from router
        let mut tools = mcp.tool_router.list_all();

        // Simulate what list_tools does: add return if schema exists
        if let Some(artifact_schema) = &mcp.artifact_schema {
            let return_tool = Tool::new(
                "return",
                "Submit final output and complete the job. The schema defines the required output fields.",
                artifact_schema.as_ref().clone(),
            );
            tools.push(return_tool);
        }

        let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();

        // return should be in the list
        assert!(
            tool_names.contains(&"return"),
            "return should be in tool list when schema is provided"
        );

        // Verify the return tool has the correct schema
        let return_tool = tools
            .iter()
            .find(|t| t.name == "return")
            .expect("return tool should exist");

        // Check schema has the expected properties
        let input_schema = return_tool.input_schema.as_ref();
        assert!(input_schema.contains_key("type"));
        assert!(input_schema.contains_key("properties"));

        let properties = input_schema.get("properties").unwrap().as_object().unwrap();
        assert!(properties.contains_key("summary"));
        assert!(properties.contains_key("confidence"));
    }

    #[test]
    fn test_list_tools_excludes_return_when_no_schema() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,   // No schema
            None,   // Default tool name
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        // Get static tools from router
        let mut tools = mcp.tool_router.list_all();

        // Simulate what list_tools does: add return if schema exists (it won't)
        if let Some(artifact_schema) = &mcp.artifact_schema {
            let return_tool = Tool::new(
                "return",
                "Submit final output and complete the job.",
                artifact_schema.as_ref().clone(),
            );
            tools.push(return_tool);
        }

        let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();

        // return should NOT be in the list
        assert!(
            !tool_names.contains(&"return"),
            "return should not be in tool list when no schema is provided"
        );
    }

    #[tokio::test]
    async fn test_handle_return_returns_error_when_no_schema() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,   // No schema
            None,   // Default tool name
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        let result = mcp.handle_return(None).await;
        assert!(result.is_ok());

        let call_result = result.unwrap();
        // Should return success with error message (not a protocol error)
        assert!(!call_result.is_error.unwrap_or(false));

        // Check the content contains the expected message
        let content_text: Option<&str> = call_result
            .content
            .first()
            .and_then(|c| c.as_text())
            .map(|t| t.text.as_ref());
        assert!(content_text.is_some());
        assert!(
            content_text.unwrap().contains("not available"),
            "Should indicate return is not available"
        );
    }

    #[test]
    fn test_schema_with_nested_properties() {
        let nested_schema: JsonSchemaObject = serde_json::from_str::<serde_json::Value>(
            r#"{
                "type": "object",
                "properties": {
                    "result": {
                        "type": "object",
                        "properties": {
                            "value": { "type": "string" },
                            "score": { "type": "number" }
                        }
                    },
                    "metadata": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                }
            }"#,
        )
        .unwrap()
        .as_object()
        .unwrap()
        .clone();

        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            Some(nested_schema),
            None,   // Default tool name
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        let stored = mcp.artifact_schema.as_ref().unwrap();
        let props = stored.get("properties").unwrap().as_object().unwrap();

        // Verify nested object
        let result_prop = props.get("result").unwrap().as_object().unwrap();
        assert_eq!(result_prop.get("type").unwrap(), "object");

        // Verify array type
        let metadata_prop = props.get("metadata").unwrap().as_object().unwrap();
        assert_eq!(metadata_prop.get("type").unwrap(), "array");
    }

    #[test]
    fn test_server_info_includes_return_in_instructions_when_schema_provided() {
        let schema = create_test_schema();
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            Some(schema),
            None,   // Default tool name
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            instructions.contains("return"),
            "Instructions should mention return when schema is provided"
        );
        assert!(
            instructions.contains("Output tools"),
            "Instructions should have Output tools section"
        );
    }

    #[test]
    fn test_server_info_excludes_return_when_no_schema() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,   // No schema
            None,   // Default tool name
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            !instructions.contains("- return:"),
            "Instructions should not mention return tool when no schema"
        );
        assert!(
            !instructions.contains("Output tools"),
            "Instructions should not have Output tools section"
        );
    }

    #[test]
    fn test_custom_tool_name() {
        let schema = create_test_schema();
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            Some(schema),
            Some("write_plan".to_string()),
            None,   // Default description
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        assert_eq!(mcp.output_tool_name, "write_plan");

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();
        assert!(
            instructions.contains("write_plan"),
            "Instructions should mention custom tool name"
        );
    }

    #[test]
    fn test_custom_tool_description() {
        let schema = create_test_schema();
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            Some(schema),
            Some("create_pr".to_string()),
            Some("Create a pull request with the given title and body".to_string()),
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        assert_eq!(mcp.output_tool_name, "create_pr");
        assert_eq!(
            mcp.output_tool_description,
            Some("Create a pull request with the given title and body".to_string())
        );
    }

    #[test]
    fn test_list_tools_modifies_task_description_with_agents() {
        let agents = vec![
            AgentInfo {
                name: "Explore".to_string(),
                description: "Search and explore the codebase".to_string(),
            },
            AgentInfo {
                name: "Research".to_string(),
                description: "Research a topic in depth".to_string(),
            },
        ];

        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,
            None,
            None,
            agents,
            vec![], // No skills
            vec![], // No custom tools
        );

        let tools = mcp.tool_router.list_all();
        let task_tool = tools.iter().find(|t| t.name == "task");
        assert!(task_tool.is_some(), "task tool should exist");

        // The static description won't be modified by tool_router.list_all()
        // That happens in list_tools(). But we can verify agents are stored.
        assert_eq!(mcp.available_agents.len(), 2);
        assert_eq!(mcp.available_agents[0].name, "Explore");
        assert_eq!(mcp.available_agents[1].name, "Research");
    }

    #[test]
    fn test_ask_user_schema_is_flat_and_inline() {
        let mcp = create_test_mcp_for_tools();
        let ask_user_tool = static_tool(&mcp, "ask_user");
        let schema = serde_json::Value::Object(ask_user_tool.input_schema.as_ref().clone());

        assert_eq!(schema.get("type").and_then(|v| v.as_str()), Some("object"));
        assert!(schema.get("$defs").is_none());

        let questions = schema
            .pointer("/properties/questions")
            .and_then(|v| v.as_object())
            .expect("questions property should exist");
        assert_eq!(
            questions.get("type").and_then(|v| v.as_str()),
            Some("array")
        );

        let items = questions
            .get("items")
            .and_then(|v| v.as_object())
            .expect("questions.items should be an object schema");
        let items_value = serde_json::Value::Object(items.clone());
        assert!(!items.contains_key("$ref"));
        assert_eq!(
            items_value
                .pointer("/properties/question/type")
                .and_then(|v| v.as_str()),
            Some("string")
        );
        assert_eq!(
            items_value
                .pointer("/properties/options/items/properties/label/type")
                .and_then(|v| v.as_str()),
            Some("string")
        );
        assert_eq!(
            items_value
                .pointer("/properties/multiSelect/type")
                .and_then(|v| v.as_str()),
            Some("boolean")
        );
    }

    #[test]
    fn test_task_schema_matches_runtime_fields() {
        let mcp = create_test_mcp_for_tools();
        let task_tool = static_tool(&mcp, "task");
        let schema = serde_json::Value::Object(task_tool.input_schema.as_ref().clone());
        let properties = schema
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("task schema should expose properties");

        assert!(properties.contains_key("subagentType"));
        assert!(properties.contains_key("tier"));
        assert!(properties.contains_key("backend"));
        assert!(properties.contains_key("session"));
        assert!(!properties.contains_key("resume"));
        assert!(!properties.contains_key("model"));

        let tier_description = properties
            .get("tier")
            .and_then(|v| v.get("description"))
            .and_then(|v| v.as_str())
            .expect("tier should include a description");
        assert!(tier_description.contains("caller's current backend"));

        let backend_description = properties
            .get("backend")
            .and_then(|v| v.get("description"))
            .and_then(|v| v.as_str())
            .expect("backend should include a description");
        assert!(backend_description.contains("claude"));
        assert!(backend_description.contains("codex"));
        assert!(backend_description.contains("Defaults to the caller's current backend"));
    }

    #[test]
    fn test_batch_tasks_schema_matches_runtime_fields() {
        let mcp = create_test_mcp_for_tools();
        let batch_tool = static_tool(&mcp, "batch_tasks");
        let schema = serde_json::Value::Object(batch_tool.input_schema.as_ref().clone());
        let task_items = schema
            .pointer("/properties/tasks/items")
            .and_then(|v| v.as_object())
            .expect("batch task items schema should exist");
        let properties = task_items
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("batch task items should expose properties");

        assert!(!task_items.contains_key("$ref"));
        assert!(properties.contains_key("tier"));
        assert!(properties.contains_key("backend"));
        assert!(properties.contains_key("session"));
        assert!(!properties.contains_key("resume"));
        assert!(!properties.contains_key("model"));
    }

    #[test]
    fn task_input_deserializes_flat_session_shape() {
        let fork: TaskInput = serde_json::from_value(serde_json::json!({
            "description": "Explore",
            "prompt": "Inspect the code",
            "subagentType": "Explore",
            "session": "fork"
        }))
        .unwrap();
        assert_eq!(fork.session.as_deref(), Some("fork"));
    }

    #[test]
    fn task_input_serializes_session_into_callback_payload() {
        let input = TaskInput {
            description: "Explore".to_string(),
            prompt: "Inspect the code".to_string(),
            subagent_type: "Explore".to_string(),
            tier: None,
            backend_preference: None,
            run_in_background: None,
            session: Some("fork".to_string()),
            task_index: None,
        };

        let payload = serde_json::to_value(&input).unwrap();
        assert_eq!(payload.get("session").and_then(|v| v.as_str()), Some("fork"));
    }

    #[test]
    fn test_server_info_includes_agents_in_instructions() {
        let agents = vec![AgentInfo {
            name: "Explore".to_string(),
            description: "Search the codebase".to_string(),
        }];

        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,
            None,
            None,
            agents,
            vec![], // No skills
            vec![], // No custom tools
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            instructions.contains("Available agents for task tool"),
            "Instructions should mention available agents"
        );
        assert!(
            instructions.contains("Explore"),
            "Instructions should include agent name"
        );
        assert!(
            instructions.contains("Search the codebase"),
            "Instructions should include agent description"
        );
    }

    #[test]
    fn test_server_info_excludes_agents_when_empty() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,
            None,
            None,
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            !instructions.contains("Available agents for task tool"),
            "Instructions should not mention agents when none available"
        );
    }

    #[test]
    fn test_server_info_includes_skills_in_instructions() {
        let skills = vec![SkillInfo {
            id: "code-review".to_string(),
            name: "Code Review".to_string(),
            description: "Guidelines for reviewing code".to_string(),
        }];

        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,
            None,
            None,
            vec![], // No agents
            skills,
            vec![], // No custom tools
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            instructions.contains("Available skills"),
            "Instructions should mention available skills"
        );
        assert!(
            instructions.contains("Code Review (`code-review`)"),
            "Instructions should show display name with id when they differ"
        );
        assert!(
            instructions.contains("Guidelines for reviewing code"),
            "Instructions should include skill description"
        );
    }

    #[test]
    fn test_skill_info_format_list_item() {
        // When name differs from id, show both
        let skill = SkillInfo {
            id: "SKILL".to_string(),
            name: "skill-making".to_string(),
            description: "Create skills".to_string(),
        };
        assert_eq!(
            skill.format_list_item(),
            "- skill-making (`SKILL`): Create skills"
        );

        // When name matches id (case-insensitive), just show id
        let skill = SkillInfo {
            id: "testing".to_string(),
            name: "testing".to_string(),
            description: "Test patterns".to_string(),
        };
        assert_eq!(skill.format_list_item(), "- testing: Test patterns");

        // Title-case name matching id also shows just id
        let skill = SkillInfo {
            id: "testing".to_string(),
            name: "Testing".to_string(),
            description: "Test patterns".to_string(),
        };
        assert_eq!(skill.format_list_item(), "- testing: Test patterns");
    }

    #[test]
    fn test_server_info_excludes_skills_when_empty() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            false,
            None,
            None,
            None,
            vec![], // No agents
            vec![], // No skills
            vec![], // No custom tools
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            !instructions.contains("Available skills"),
            "Instructions should not mention skills when none available"
        );
    }

    // --- cap_text_result / paginate_text tests ---

    #[test]
    fn test_cap_text_result_short_text_unchanged() {
        let text = "line 1\nline 2\nline 3";
        let result = cap_text_result(text, 0);
        assert_eq!(result, text);
    }

    #[test]
    fn test_cap_text_result_truncates_at_line_boundary() {
        // Build text that exceeds MAX_RESULT_CHARS
        let line = "x".repeat(100);
        let lines: Vec<&str> = std::iter::repeat(line.as_str())
            .take(MAX_RESULT_CHARS / 100 + 100)
            .collect();
        let text = lines.join("\n");
        assert!(text.len() > MAX_RESULT_CHARS);

        let result = cap_text_result(&text, 0);

        // Should be truncated
        assert!(result.len() < text.len());
        // Should contain continuation hint
        assert!(result.contains("--- truncated"));
        assert!(result.contains("Call again with offset="));
        // The truncated content should end at a line boundary (before the hint)
        let content_part = result.split("\n\n--- truncated").next().unwrap();
        // Each line is 100 chars, so content should be made of complete lines
        assert!(content_part.lines().all(|l| l.len() == 100));
    }

    #[test]
    fn test_cap_text_result_with_nonzero_offset() {
        let line = "x".repeat(100);
        let lines: Vec<&str> = std::iter::repeat(line.as_str())
            .take(MAX_RESULT_CHARS / 100 + 100)
            .collect();
        let text = lines.join("\n");

        let result = cap_text_result(&text, 50);
        // Continuation hint should show offset-based line numbers
        assert!(result.contains("lines 51-"));
        assert!(result.contains("Call again with offset="));
    }

    #[test]
    fn test_paginate_text_no_params() {
        let text = "line 1\nline 2\nline 3";
        let result = paginate_text(text, None, None);
        assert_eq!(result, text);
    }

    #[test]
    fn test_paginate_text_with_offset() {
        let text = "line 0\nline 1\nline 2\nline 3\nline 4";
        let result = paginate_text(text, Some(2), None);
        assert_eq!(result, "line 2\nline 3\nline 4");
    }

    #[test]
    fn test_paginate_text_with_limit() {
        let text = "line 0\nline 1\nline 2\nline 3\nline 4";
        let result = paginate_text(text, None, Some(3));
        assert_eq!(result, "line 0\nline 1\nline 2");
    }

    #[test]
    fn test_paginate_text_with_offset_and_limit() {
        let text = "line 0\nline 1\nline 2\nline 3\nline 4";
        let result = paginate_text(text, Some(1), Some(2));
        assert_eq!(result, "line 1\nline 2");
    }

    #[test]
    fn test_paginate_text_offset_past_end() {
        let text = "line 0\nline 1";
        let result = paginate_text(text, Some(10), None);
        assert!(result.contains("Offset 10 is past end of content"));
    }

    #[test]
    fn test_paginate_text_limit_exceeds_remaining() {
        let text = "line 0\nline 1\nline 2";
        let result = paginate_text(text, Some(1), Some(100));
        assert_eq!(result, "line 1\nline 2");
    }

    #[test]
    fn test_paginate_text_caps_large_result() {
        // Build large text, paginate without limit — should still be capped
        let line = "x".repeat(100);
        let lines: Vec<&str> = std::iter::repeat(line.as_str())
            .take(MAX_RESULT_CHARS / 100 + 100)
            .collect();
        let text = lines.join("\n");

        let result = paginate_text(&text, None, None);
        assert!(result.contains("--- truncated"));
        assert!(result.contains("Call again with offset="));
    }

    #[test]
    fn test_parse_tool_response_caps_large_output() {
        let line = "x".repeat(100);
        let lines: Vec<&str> = std::iter::repeat(line.as_str())
            .take(MAX_RESULT_CHARS / 100 + 100)
            .collect();
        let large_output = lines.join("\n");

        let raw = serde_json::json!({
            "output": large_output,
            "isError": false
        })
        .to_string();

        let result = parse_tool_response(&raw);
        let text: &str = result
            .content
            .first()
            .unwrap()
            .as_text()
            .unwrap()
            .text
            .as_ref();
        assert!(text.contains("--- truncated"));
    }

    #[test]
    fn test_parse_tool_response_caps_raw_fallback() {
        // When the raw string isn't valid ToolResponse JSON, it's used as-is but still capped
        let line = "x".repeat(100);
        let lines: Vec<&str> = std::iter::repeat(line.as_str())
            .take(MAX_RESULT_CHARS / 100 + 100)
            .collect();
        let large_raw = lines.join("\n");

        let result = parse_tool_response(&large_raw);
        let text: &str = result
            .content
            .first()
            .unwrap()
            .as_text()
            .unwrap()
            .text
            .as_ref();
        assert!(text.contains("--- truncated"));
    }

    #[test]
    fn test_paginate_text_empty_string() {
        // Empty text with default params should return empty, not an error
        let result = paginate_text("", None, None);
        assert_eq!(result, "");
    }

    #[test]
    fn test_cap_text_result_no_newlines() {
        // A single long line with no newlines should still truncate
        let text = "x".repeat(MAX_RESULT_CHARS + 1000);
        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // Should truncate at MAX_RESULT_CHARS since no newline found
        assert!(result.starts_with(&"x".repeat(MAX_RESULT_CHARS)));
    }

    #[test]
    fn test_cap_text_result_multibyte_utf8_boundary() {
        // Build text where MAX_RESULT_CHARS falls inside a multi-byte char.
        // U+00E9 (é) is 2 bytes in UTF-8 (0xC3 0xA9).
        // Fill with ASCII up to one byte before the limit, then a 2-byte char
        // so the limit lands on the second byte of the é.
        let prefix = "a".repeat(MAX_RESULT_CHARS - 1);
        // é adds 2 bytes → total = MAX_RESULT_CHARS + 1 bytes, triggers truncation
        // and MAX_RESULT_CHARS falls on byte index inside the é
        let text = format!("{}é{}", prefix, "b".repeat(2000));
        assert!(text.len() > MAX_RESULT_CHARS);

        // Before the fix this would panic with "byte index is not a char boundary"
        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // The é should be excluded (boundary rounds down), so content is just the prefix
        assert!(result.starts_with(&prefix));
    }

    #[test]
    fn test_cap_text_result_multibyte_utf8_with_newlines() {
        // Mix multi-byte chars with newlines to test both boundary + line truncation
        // Each line: 99 ASCII chars + é (2 bytes) + newline = 102 bytes per line
        let line = format!("{}é", "z".repeat(99));
        assert_eq!(line.len(), 101); // 99 + 2 bytes for é
        let num_lines = MAX_RESULT_CHARS / 101 + 100;
        let text = std::iter::repeat(line.as_str())
            .take(num_lines)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.len() > MAX_RESULT_CHARS);

        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // Content before the truncation marker should end at a complete line
        let content_part = result.split("\n\n--- truncated").next().unwrap();
        for line in content_part.lines() {
            assert!(line.ends_with('é'), "Line should be intact: {:?}", line);
        }
    }

    #[test]
    fn test_image_response_parse_rejects_source_code_containing_is_image() {
        // Source code that contains the literal string "is_image":true should NOT
        // parse as a valid ImageResponse (it lacks mime_type and data fields).
        // This was the false positive that the removed content-scanning fallback triggered.
        let source_code = r#"
            struct ImageResponse {
                is_image: bool,
                mime_type: String,
                data: String,
            }
            // check: "is_image":true and "is_image": true
        "#;
        let parsed = serde_json::from_str::<ImageResponse>(source_code);
        assert!(
            parsed.is_err(),
            "Source code must not parse as ImageResponse"
        );
    }

    #[test]
    fn test_image_response_parse_accepts_valid_image_json() {
        // A properly structured image response should still parse correctly
        let json = r#"{"is_image":true,"mime_type":"image/png","data":"iVBORw0KGgo="}"#;
        let parsed = serde_json::from_str::<ImageResponse>(json).unwrap();
        assert!(parsed.is_image);
        assert_eq!(parsed.mime_type, "image/png");
        assert_eq!(parsed.data, "iVBORw0KGgo=");
    }

    #[test]
    fn test_image_response_parse_with_is_image_false() {
        // When is_image is false, it parses but the guard pattern won't match
        let json = r#"{"is_image":false,"mime_type":"","data":""}"#;
        let parsed = serde_json::from_str::<ImageResponse>(json).unwrap();
        assert!(!parsed.is_image);
    }

    #[test]
    fn test_parse_tool_response_caps_error_output() {
        let line = "x".repeat(100);
        let lines: Vec<&str> = std::iter::repeat(line.as_str())
            .take(MAX_RESULT_CHARS / 100 + 100)
            .collect();
        let large_output = lines.join("\n");

        let raw = serde_json::json!({
            "output": large_output,
            "isError": true
        })
        .to_string();

        let result = parse_tool_response(&raw);
        assert!(result.is_error.unwrap_or(false));
        let text: &str = result
            .content
            .first()
            .unwrap()
            .as_text()
            .unwrap()
            .text
            .as_ref();
        assert!(text.contains("--- truncated"));
    }

    #[test]
    fn test_read_uri_excluded_from_internal_mode() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            false, // internal mode
            None,
            None,
            None,
            vec![],
            vec![],
            vec![],
        );

        let tools = mcp.tool_router.list_all();
        let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(
            tool_names.contains(&"read_uri"),
            "read_uri should be in router (filtering happens in list_tools)"
        );
        // Verify that internal mode has 'read' tool
        assert!(
            tool_names.contains(&"read"),
            "read should be in internal mode tools"
        );
    }

    #[test]
    fn test_read_uri_present_in_external_mode() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            true, // external mode
            None,
            None,
            None,
            vec![],
            vec![],
            vec![],
        );

        let tools = mcp.tool_router.list_all();
        let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(
            tool_names.contains(&"read_uri"),
            "read_uri should be in external mode tools"
        );
    }

    fn create_test_mcp(cwd: &str) -> CairnMcp {
        CairnMcp::new(
            "http://localhost:3847".to_string(),
            cwd.to_string(),
            None,
            None,
            false,
            None,
            None,
            None,
            vec![],
            vec![],
            vec![],
        )
    }

    fn create_external_mcp(cwd: &str) -> CairnMcp {
        CairnMcp::new(
            "http://localhost:3847".to_string(),
            cwd.to_string(),
            None,
            None,
            true, // external mode
            None,
            None,
            None,
            vec![],
            vec![],
            vec![],
        )
    }

    fn get_text(result: &CallToolResult) -> &str {
        result
            .content
            .first()
            .and_then(|c| c.as_text())
            .map(|t| t.text.as_ref())
            .unwrap()
    }

    // --- Glob tool tests ---
    // NOTE: Glob and grep logic now lives in the Tauri callback handler
    // (cairn-core mcp/handlers). Functional tests for glob/grep belong there.
    // Only access-control tests (external mode rejection) remain here.

    #[tokio::test]
    async fn test_glob_rejected_in_external_mode() {
        let mcp = create_external_mcp("/tmp");
        let result = mcp
            .glob(Parameters(GlobInput {
                pattern: "*.rs".to_string(),
                path: None,
            }))
            .await
            .unwrap();

        let text = get_text(&result);
        assert!(
            text.contains("not available in external mode"),
            "should reject in external mode: {text}"
        );
    }

    // --- Grep tool tests ---

    #[tokio::test]
    async fn test_grep_rejected_in_external_mode() {
        let mcp = create_external_mcp("/tmp");
        let result = mcp
            .grep(Parameters(GrepInput {
                pattern: "test".to_string(),
                path: None,
                glob: None,
                file_type: None,
                output_mode: None,
                context: None,
                after_context: None,
                before_context: None,
                context_alias: None,
                case_insensitive: None,
                line_numbers: None,
                head_limit: None,
                offset: None,
                multiline: None,
            }))
            .await
            .unwrap();

        let text = get_text(&result);
        assert!(
            text.contains("not available in external mode"),
            "should reject in external mode: {text}"
        );
    }

    // ========================================================================
    // Web tools tests
    // ========================================================================

    #[test]
    fn test_strip_boilerplate_removes_script_tags() {
        let html =
            r#"<html><body><p>Hello</p><script>alert('xss')</script><p>World</p></body></html>"#;
        let result = strip_boilerplate(html);
        assert!(!result.contains("alert"));
        assert!(result.contains("Hello"));
        assert!(result.contains("World"));
    }

    #[test]
    fn test_strip_boilerplate_removes_style_tags() {
        let html = r#"<html><body><style>.foo { color: red; }</style><p>Content</p></body></html>"#;
        let result = strip_boilerplate(html);
        assert!(!result.contains("color: red"));
        assert!(result.contains("Content"));
    }

    #[test]
    fn test_strip_boilerplate_removes_nav_footer_header() {
        let html = r#"<html><body><header>Site Header</header><nav>Menu</nav><p>Main content</p><footer>Copyright</footer></body></html>"#;
        let result = strip_boilerplate(html);
        assert!(!result.contains("Site Header"));
        assert!(!result.contains("Menu"));
        assert!(!result.contains("Copyright"));
        assert!(result.contains("Main content"));
    }

    #[test]
    fn test_strip_boilerplate_preserves_content_without_boilerplate() {
        let html = r#"<html><body><h1>Title</h1><p>Paragraph</p></body></html>"#;
        let result = strip_boilerplate(html);
        assert!(result.contains("Title"));
        assert!(result.contains("Paragraph"));
    }

    #[test]
    fn test_extract_title() {
        assert_eq!(
            extract_title("<html><head><title>My Page</title></head></html>"),
            Some("My Page".to_string())
        );
        assert_eq!(extract_title("<html><head></head></html>"), None);
        assert_eq!(
            extract_title("<html><head><title>  </title></head></html>"),
            None
        );
    }

    #[test]
    fn test_score_section() {
        let section = "This section talks about Rust programming and web development.";
        let keywords = vec!["rust", "web", "python"];
        assert_eq!(score_section(section, &keywords), 2); // "rust" and "web" match

        let empty_section = "Nothing relevant here.";
        assert_eq!(score_section(empty_section, &keywords), 0);
    }

    #[test]
    fn test_smart_truncate_returns_short_content_unchanged() {
        let content = "Short content";
        let result = smart_truncate(content, "anything", 1000);
        assert_eq!(result, content);
    }

    #[test]
    fn test_smart_truncate_keeps_relevant_sections() {
        let content = "# Introduction\nGeneral overview of the project.\n\n# Rust Guide\nThis section covers Rust programming patterns.\n\n# Python Guide\nThis section covers Python patterns.\n\n# Conclusion\nFinal thoughts.";
        // With a limit that can't fit everything, the Rust section should be prioritized
        let result = smart_truncate(content, "Rust programming", 120);
        assert!(result.contains("Rust"));
    }

    #[test]
    fn test_smart_truncate_with_no_headings() {
        let content = "A".repeat(200);
        let result = smart_truncate(&content, "anything", 100);
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_hash_prompt_deterministic() {
        assert_eq!(hash_prompt("test"), hash_prompt("test"));
        assert_ne!(hash_prompt("test"), hash_prompt("other"));
    }

    #[test]
    fn test_html_to_markdown_basic() {
        let html = "<h1>Title</h1><p>Hello <strong>world</strong></p>";
        let md = html_to_markdown(html);
        assert!(md.contains("Title"));
        assert!(md.contains("world"));
    }

    #[test]
    fn test_normalize_url_upgrades_http() {
        let url = normalize_url("http://example.com").unwrap();
        assert_eq!(url.scheme(), "https");
    }

    #[test]
    fn test_normalize_url_adds_scheme() {
        let url = normalize_url("example.com").unwrap();
        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("example.com"));
    }

    #[test]
    fn test_normalize_url_keeps_https() {
        let url = normalize_url("https://example.com/path").unwrap();
        assert_eq!(url.as_str(), "https://example.com/path");
    }

    #[test]
    fn test_normalize_url_rejects_invalid() {
        assert!(normalize_url("not a url at all %%%").is_err());
    }

    #[test]
    fn test_extract_domain() {
        let url = url::Url::parse("https://docs.rust-lang.org/book/").unwrap();
        assert_eq!(extract_domain(&url), "docs.rust-lang.org");
    }

    // ========================================================================
    // Web cache tests
    // ========================================================================

    #[test]
    fn test_web_cache_put_and_get() {
        let mut cache = WebCache::new();
        cache.put("https://example.com", 123, "result".to_string());
        assert_eq!(cache.get("https://example.com", 123), Some("result"));
    }

    #[test]
    fn test_web_cache_miss() {
        let mut cache = WebCache::new();
        assert_eq!(cache.get("https://example.com", 123), None);
    }

    #[test]
    fn test_web_cache_different_prompts() {
        let mut cache = WebCache::new();
        cache.put("https://example.com", 1, "result1".to_string());
        cache.put("https://example.com", 2, "result2".to_string());
        assert_eq!(cache.get("https://example.com", 1), Some("result1"));
        assert_eq!(cache.get("https://example.com", 2), Some("result2"));
    }

    #[test]
    fn test_web_cache_eviction() {
        let mut cache = WebCache::new();
        // Fill cache to max
        for i in 0..100 {
            cache.put(&format!("https://example.com/{}", i), 0, format!("r{}", i));
        }
        assert_eq!(cache.entries.len(), 100);

        // Adding one more should evict the oldest
        cache.put("https://example.com/new", 0, "new".to_string());
        assert!(cache.entries.len() <= 100);
        assert_eq!(cache.get("https://example.com/new", 0), Some("new"));
    }

    // ========================================================================
    // Rate limiter tests
    // ========================================================================

    #[test]
    fn test_rate_limiter_allows_requests() {
        let mut limiter = RateLimiter::new();
        for _ in 0..10 {
            assert!(limiter.check("example.com").is_ok());
        }
    }

    #[test]
    fn test_rate_limiter_blocks_excess_requests() {
        let mut limiter = RateLimiter::new();
        for _ in 0..10 {
            assert!(limiter.check("example.com").is_ok());
        }
        // 11th request should be blocked
        assert!(limiter.check("example.com").is_err());
    }

    #[test]
    fn test_rate_limiter_different_domains() {
        let mut limiter = RateLimiter::new();
        for _ in 0..10 {
            assert!(limiter.check("example.com").is_ok());
        }
        // Different domain should still be allowed
        assert!(limiter.check("other.com").is_ok());
    }

    // ========================================================================
    // Additional web tool tests (Proctor)
    // ========================================================================

    #[test]
    fn test_web_cache_overwrite_existing_key() {
        let mut cache = WebCache::new();
        cache.put("https://example.com", 42, "old_result".to_string());
        cache.put("https://example.com", 42, "new_result".to_string());
        assert_eq!(cache.get("https://example.com", 42), Some("new_result"));
    }

    #[test]
    fn test_rate_limiter_returns_wait_time() {
        let mut limiter = RateLimiter::new();
        for _ in 0..10 {
            limiter.check("example.com").unwrap();
        }
        // 11th request should return Err with a wait time <= 60
        let err = limiter.check("example.com").unwrap_err();
        assert!(err <= 60, "wait time should be at most 60s, got {}", err);
        assert!(err > 0, "wait time should be positive, got {}", err);
    }

    #[test]
    fn test_strip_boilerplate_removes_noscript() {
        let html = "<html><body><noscript>Enable JS</noscript><p>Content</p></body></html>";
        let result = strip_boilerplate(html);
        assert!(
            !result.contains("Enable JS"),
            "noscript content should be stripped"
        );
        assert!(result.contains("Content"));
    }

    #[test]
    fn test_strip_boilerplate_handles_tags_with_attributes() {
        let html = r#"<html><body><script type="text/javascript" src="app.js">var x=1;</script><p>Keep this</p></body></html>"#;
        let result = strip_boilerplate(html);
        assert!(
            !result.contains("var x=1"),
            "script with attributes should be stripped"
        );
        assert!(result.contains("Keep this"));
    }

    #[test]
    fn test_strip_boilerplate_multiple_same_tag() {
        let html = "<html><body><script>a</script><p>mid</p><script>b</script></body></html>";
        let result = strip_boilerplate(html);
        assert!(
            !result.contains("a") || result.contains("html"),
            "first script stripped"
        );
        assert!(
            !result.contains("<script>"),
            "all script tags should be removed"
        );
        assert!(result.contains("mid"));
    }

    #[test]
    fn test_smart_truncate_preserves_document_order() {
        // Section B scores higher, but should appear after A in output (original order)
        let content = "# Section A\nSome general intro text.\n\n# Section B\nThis covers Rust and Go programming.\n\n# Section C\nUnrelated content.";
        let result = smart_truncate(content, "Rust Go programming", 200);
        // B should be present since it's most relevant
        assert!(result.contains("Rust"));
        // If A is also present (fits within limit), A should come before B
        if result.contains("Section A") && result.contains("Section B") {
            let a_pos = result.find("Section A").unwrap();
            let b_pos = result.find("Section B").unwrap();
            assert!(
                a_pos < b_pos,
                "Section A should precede Section B in document order"
            );
        }
    }

    #[test]
    fn test_smart_truncate_short_keyword_fallback() {
        // All prompt words are <= 2 chars, so keywords is empty -> plain truncation
        let content = "# A\nFirst section.\n\n# B\nSecond section.";
        let result = smart_truncate(content, "it is", 20);
        // Should just truncate from the start since no keywords matched
        assert_eq!(result.len(), 20);
    }

    #[test]
    fn test_normalize_url_preserves_query_and_fragment() {
        let url = normalize_url("https://example.com/path?q=rust&page=1#section").unwrap();
        assert_eq!(url.query(), Some("q=rust&page=1"));
        assert_eq!(url.fragment(), Some("section"));
    }

    #[test]
    fn test_html_to_markdown_strips_boilerplate() {
        // html_to_markdown calls strip_boilerplate internally
        let html = "<html><body><nav>Menu</nav><h1>Title</h1><script>evil()</script><p>Content</p></body></html>";
        let md = html_to_markdown(html);
        assert!(md.contains("Title"), "main content should be present");
        assert!(md.contains("Content"), "paragraph should be present");
        assert!(!md.contains("evil"), "script should be stripped");
        assert!(!md.contains("Menu"), "nav should be stripped");
    }

    #[tokio::test]
    async fn test_web_fetch_rejected_in_external_mode() {
        let mcp = create_external_mcp("/tmp");
        let result = mcp
            .web_fetch(Parameters(WebFetchInput {
                url: "https://example.com".to_string(),
                prompt: "test".to_string(),
                raw: None,
                max_length: None,
            }))
            .await
            .unwrap();

        let text = get_text(&result);
        assert!(
            text.contains("not available in external mode"),
            "web_fetch should be rejected in external mode: {text}"
        );
    }

    #[tokio::test]
    async fn test_web_search_rejected_in_external_mode() {
        let mcp = create_external_mcp("/tmp");
        let result = mcp
            .web_search(Parameters(WebSearchInput {
                query: "test query".to_string(),
                limit: None,
                allowed_domains: None,
                blocked_domains: None,
            }))
            .await
            .unwrap();

        let text = get_text(&result);
        assert!(
            text.contains("not available in external mode"),
            "web_search should be rejected in external mode: {text}"
        );
    }

    #[test]
    fn test_web_fetch_and_web_search_tools_registered() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            false,
            None,
            None,
            None,
            vec![],
            vec![],
            vec![],
        );

        let tools = mcp.tool_router.list_all();
        let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(
            tool_names.contains(&"web_fetch"),
            "web_fetch should be registered"
        );
        assert!(
            tool_names.contains(&"web_search"),
            "web_search should be registered"
        );
    }

    #[test]
    fn test_unified_edit_tool_visible() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            false,
            None,
            None,
            None,
            vec![],
            vec![],
            vec![],
        );

        let all_tools = mcp.tool_router.list_all();
        let all_names: Vec<&str> = all_tools.iter().map(|t| t.name.as_ref()).collect();

        assert!(
            all_names.contains(&"edit"),
            "edit tool should be in tool router"
        );
        // write and filechange should NOT exist
        assert!(
            !all_names.contains(&"write"),
            "write tool should not exist (unified into edit)"
        );
        assert!(
            !all_names.contains(&"filechange"),
            "filechange tool should not exist (unified into edit)"
        );
    }
}
