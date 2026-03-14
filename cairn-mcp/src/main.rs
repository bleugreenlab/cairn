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
use std::env;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

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

use cairn_common::auth::{decode_secret, generate_passcode, SECRET_LEN};
use cairn_common::protocol::{CallbackRequest, CallbackResponse};

/// Cairn MCP Server - tools for Claude to interact with Cairn during planning
#[derive(Clone)]
struct CairnMcp {
    callback_url: Arc<String>,
    /// Current working directory - used by backend to identify the active run
    cwd: Arc<String>,
    /// Run ID - preferred method to identify the active run (avoids cwd ambiguity)
    run_id: Option<Arc<String>>,
    /// Shared secret for TOTP-style passcode generation (base64-decoded from env var)
    mcp_secret: Option<Arc<[u8; SECRET_LEN]>>,
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
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct AskUserInput {
    /// The questions to ask the user
    questions: Vec<Question>,
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

/// Input for write tool (replaces native Write)
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct WriteFileInput {
    /// Path to the file, relative to the worktree root
    file_path: String,
    /// The content to write to the file
    content: String,
    /// Commit message for this change. Use "^" to amend to the previous commit.
    /// Use "NO_COMMIT" to write the file without staging or committing.
    commit_msg: String,
}

/// Input for edit tool (replaces native Edit)
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct EditFileInput {
    /// Path to the file, relative to the worktree root
    file_path: String,
    /// The text to find and replace. For blocks longer than a few lines, prefer wildcard
    /// matching over copying the unchanged middle: put a line containing only '~~~~~' between
    /// two anchor strings. Example: "fn foo() {\n~~~~~\n}" replaces from "fn foo() {" through
    /// the matching '}'. Multiple '~~~~~' lines are supported — intermediate text acts as
    /// additional anchors that must match in sequence. Delimiter-aware: '{', '[', '(' opened
    /// in the head are tracked by depth, so '~~~~~' skips over nested delimiters and the tail
    /// always exits at the correct closer regardless of whitespace. Anchors only need to be
    /// unique in the file — one recognizable line on each side is enough; never copy large
    /// unchanged sections verbatim.
    old_string: String,
    /// The replacement text
    new_string: String,
    /// Replace all occurrences (default: false)
    #[serde(default)]
    replace_all: bool,
    /// Commit message for this change. Use "^" to amend to the previous commit.
    /// Use "NO_COMMIT" to write the file without staging or committing.
    commit_msg: String,
}

/// Input for read_uri tool (external-mode friendly, cairn:// URIs only)
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ReadUriInput {
    /// Cairn resource URI (e.g., cairn://PROJECT/NUMBER, cairn://PROJECT/NUMBER/EXEC/NODE/chat)
    uri: String,
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
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TaskInput {
    /// A short (3-5 word) description of the task
    description: String,
    /// The task for the agent to perform
    prompt: String,
    /// The type of specialized agent to use (agent config ID)
    subagent_type: String,
    /// Optional model override: "sonnet", "opus", or "haiku"
    model: Option<String>,
    /// Set to true to run this agent in the background
    run_in_background: Option<bool>,
    /// Optional agent ID to resume from
    resume: Option<String>,
    /// Task index for batch_tasks ordering (0, 1, 2...) - set internally by batch_tasks
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    task_index: Option<i32>,
}

/// Input for batch_tasks tool - execute multiple tasks in parallel
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct BatchTasksInput {
    /// Array of tasks to execute in parallel
    tasks: Vec<TaskInput>,
}

/// Input for skill tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SkillInput {
    /// Skill ID or name to retrieve
    skill_id: String,
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
    triggers: Vec<TriggerCondition>,
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

/// Response format from execute/custom_tool handlers.
/// Contains formatted output and error flag for MCP tool results.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolResponse {
    output: String,
    is_error: bool,
}

/// Parse a ToolResponse from a callback result string.
/// Falls back to treating the raw string as successful output.
fn parse_tool_response(raw: &str) -> CallToolResult {
    match serde_json::from_str::<ToolResponse>(raw) {
        Ok(resp) if resp.is_error => CallToolResult::error(vec![Content::text(resp.output)]),
        Ok(resp) => CallToolResult::success(vec![Content::text(resp.output)]),
        Err(_) => {
            // Fallback: treat raw string as successful text output
            CallToolResult::success(vec![Content::text(raw)])
        }
    }
}

#[tool_router]
impl CairnMcp {
    fn new(
        callback_url: String,
        cwd: String,
        run_id: Option<String>,
        mcp_secret: Option<[u8; SECRET_LEN]>,
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
            mcp_secret: mcp_secret.map(Arc::new),
            external_mode,
            tool_router: Self::tool_router(),
            artifact_schema: artifact_schema.map(Arc::new),
            output_tool_name: output_tool_name.unwrap_or_else(|| "return".to_string()),
            output_tool_description,
            available_agents,
            available_skills,
            available_tools,
            report_count: Arc::new(AtomicU32::new(0)),
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

    /// Read a resource URI and return tool result.
    /// Handles cairn:// URI scheme.
    async fn read_resource_uri(&self, uri: &str) -> Result<CallToolResult, rmcp::ErrorData> {
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
                return Ok(CallToolResult::success(vec![Content::text(
                    terminal_result.output,
                )]));
            }
        }

        // For issue resources (or fallback), return the result directly
        Ok(CallToolResult::success(vec![Content::text(result)]))
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

        // Notify Tauri to store prompt and kill Claude - we don't return
        let _ = self.call_tauri(&request).await;

        // Block forever - Tauri will kill Claude (and us), we never respond
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
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

    /// Write content to a file and commit the change.
    /// Each write is automatically committed. Use commit_msg="^" to amend to the previous commit
    /// for multi-file atomic changes or corrections.
    #[tool(description = "Write content to a file and commit the change")]
    async fn write(
        &self,
        params: Parameters<WriteFileInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("write") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!(
            "write called: {} ({} chars, msg: {})",
            input.file_path,
            input.content.len(),
            if input.commit_msg == "^" {
                "amend"
            } else {
                &input.commit_msg
            }
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "write".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Edit a file by replacing text and commit the change.
    /// Each edit is automatically committed. Use commit_msg="^" to amend to the previous commit
    /// for multi-file atomic changes or corrections.
    #[tool(description = "Edit a file by replacing text and commit the change")]
    async fn edit(
        &self,
        params: Parameters<EditFileInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(msg) = self.require_internal("edit") {
            return Ok(CallToolResult::success(vec![Content::text(msg)]));
        }
        let input = params.0;
        tracing::info!(
            "edit called: {} (replace {}→{} chars, msg: {})",
            input.file_path,
            input.old_string.len(),
            input.new_string.len(),
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
            return self.read_resource_uri(&input.path).await;
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

        // Safety: if it looks like image JSON but we couldn't process it, don't dump base64
        if result.contains("\"is_image\":true") || result.contains("\"is_image\": true") {
            tracing::error!("Image JSON detected but failed to process - preventing base64 dump");
            return Ok(CallToolResult::success(vec![Content::text(
                "Error: Failed to process image content. The file appears to be an image but could not be rendered.",
            )]));
        }

        // Fall through to text content
        Ok(CallToolResult::success(vec![Content::text(result)]))
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
        let input = params.0;
        tracing::info!("read_uri called: {}", input.uri);
        self.read_resource_uri(&input.uri).await
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
        Ok(CallToolResult::success(vec![Content::text(result)]))
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
        tracing::info!(
            "bash called: {} (bg={})",
            &input.command[..input.command.len().min(100)],
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
        Ok(CallToolResult::success(vec![Content::text(result)]))
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
            "[DEBUG-TASK] task called: request_id={} subagent_type={}, description={}, prompt_len={}",
            request_id,
            &input.subagent_type,
            &input.description,
            input.prompt.len()
        );

        // Single task calls don't have a parent tool_use_id
        tracing::info!(
            "[DEBUG-TASK] spawning single task request_id={}",
            request_id
        );
        let result = self.spawn_single_task(&input, None).await;
        tracing::info!(
            "[DEBUG-TASK] task handler COMPLETED request_id={}",
            request_id
        );
        Ok(CallToolResult::success(vec![Content::text(result)]))
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

        // Format combined output
        let output = results
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

        Ok(CallToolResult::success(vec![Content::text(output)]))
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
        tracing::info!("skill called: {}", input.skill_id);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "skill".to_string(),
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

    /// Create a new memory with trigger conditions.
    /// Memories surface as contextual footnotes when agents use tools matching the triggers.
    #[tool(description = "Create a memory with trigger conditions for contextual surfacing")]
    async fn create_memory(
        &self,
        params: Parameters<CreateMemoryInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!(
            "create_memory called: {} ({} triggers)",
            &input.content[..input.content.len().min(50)],
            input.triggers.len()
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "create_memory".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
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
}

impl CairnMcp {
    /// Spawn a single task agent and wait for its result.
    /// Used by both `task` and `batch_tasks` handlers.
    ///
    /// # Arguments
    /// * `input` - The task input parameters
    /// * `parent_tool_use_id` - For batch_tasks: the parent's tool_use_id to link child jobs
    async fn spawn_single_task(
        &self,
        input: &TaskInput,
        parent_tool_use_id: Option<&str>,
    ) -> String {
        let spawn_id = uuid::Uuid::new_v4();
        tracing::info!("[DEBUG-SPAWN] spawn_single_task ENTERED spawn_id={} subagent_type={} parent_tool_use_id={:?}", 
            spawn_id, input.subagent_type, parent_tool_use_id);

        // Log warnings for unsupported features
        if input.run_in_background.unwrap_or(false) {
            tracing::warn!("run_in_background is not yet supported for Cairn task tool");
        }
        if input.resume.is_some() {
            tracing::warn!("resume is not yet supported for Cairn task tool");
        }

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "task".to_string(),
            payload: serde_json::to_value(input).unwrap_or_default(),
            tool_use_id: parent_tool_use_id.map(|s| s.to_string()),
        };

        tracing::info!(
            "[DEBUG-SPAWN] Calling Tauri callback spawn_id={} subagent_type={}",
            spawn_id,
            input.subagent_type
        );
        let result = self.call_tauri(&request).await;
        tracing::info!(
            "[DEBUG-SPAWN] spawn_single_task COMPLETED spawn_id={} subagent_type={}",
            spawn_id,
            input.subagent_type
        );
        result
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
        let client = reqwest::Client::new();
        let mut req = client.post(self.callback_url.as_str()).json(request);

        // Generate fresh passcode for each request (TOTP-style authentication)
        if let Some(secret) = &self.mcp_secret {
            let passcode = generate_passcode(secret);
            req = req.header("Authorization", format!("Bearer {}", passcode));
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
             - write: Write file content and auto-commit (use commit_msg=\"^\" to amend)\n\
             - edit: Edit file with find/replace and auto-commit (use commit_msg=\"^\" to amend)\n\
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
                instructions.push_str(&format!("- {}: {}\n", skill.id, skill.description));
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
                            desc.push_str(&format!("- {}: {}\n", skill.id, skill.description));
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
                        let contents = vec![ResourceContents::text(terminal_result.output, uri)];
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
            let contents = vec![ResourceContents::text(result, uri)];
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

    // Initialize tracing to stderr (stdout is used for MCP protocol)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(std::io::stderr)
        .init();

    // Callback URL - passed from main app via MCP config env var
    let callback_url = env::var("CAIRN_CALLBACK_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3847/api/mcp".to_string());
    // Get current working directory - fallback for run identification
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    // Run ID - preferred method for accurate run identification (avoids cwd ambiguity)
    let run_id = env::var("CAIRN_RUN_ID").ok();
    // Shared secret for TOTP-style passcode generation (base64-encoded)
    let mcp_secret = env::var("CAIRN_MCP_SECRET")
        .ok()
        .and_then(|s| decode_secret(&s));

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
            instructions.contains("code-review"),
            "Instructions should include skill id"
        );
        assert!(
            instructions.contains("Guidelines for reviewing code"),
            "Instructions should include skill description"
        );
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
}
