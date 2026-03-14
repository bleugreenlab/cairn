use crate::config::agents as config_agents;
use crate::models::AgentConfig;
use crate::orchestrator::Orchestrator;
use crate::schema::{jobs, projects};
use cairn_common::protocol::CallbackRequest;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tempfile::NamedTempFile;

use super::{lookup_run, RunContext};

/// TOTP time step in seconds - must match auth.rs
const TOTP_TIME_STEP_SECS: u64 = 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutePayload {
    pub code: String,
    #[serde(default)]
    pub timeout: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResult {
    /// Captured return value from user code (written to result file)
    pub result: Option<String>,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
}

/// Formatted response sent back to cairn-mcp for tool display.
/// cairn-mcp parses this to set output content and isError flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolResponse {
    pub output: String,
    pub is_error: bool,
}

pub fn handle_execute_sync(orch: &Orchestrator, request: &CallbackRequest) -> String {
    let payload: ExecutePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!("execute called with {} chars of code", payload.code.len());

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Connection error: {}", e),
    };

    let ctx = match lookup_run(&mut conn, request) {
        Ok(c) => c,
        Err(e) => return e,
    };

    let mcp_secret = orch.mcp_auth.get_secret_for_mcp().unwrap_or_default();

    let allowed_tools = get_allowed_tools_for_run(&mut conn, &ctx, &orch.config_dir);

    let project_path: Option<String> = projects::table
        .find(&ctx.project_id)
        .select(projects::repo_path)
        .first(&mut *conn)
        .ok();

    drop(conn);

    let cwd = project_path.as_deref().unwrap_or(&request.cwd);

    match execute_code(
        &ctx,
        cwd,
        &payload,
        &mcp_secret,
        &allowed_tools,
        orch.mcp_callback_port,
    ) {
        Ok(result) => format_tool_response(&result),
        Err(e) => format_error(&e),
    }
}

/// Tools that actually exist on the MCP callback server.
/// These are the only tools that should be exposed in the execute runtime.
const CALLBACK_SERVER_TOOLS: &[&str] = &[
    "read",
    "write",
    "edit",
    "bash",
    "kill_shell",
    "task",
    "batch_tasks",
    "skill",
    "search",
    "create_issue",
    "update_issue",
    "add_comment",
    "ask_user",
];

fn get_default_tools() -> HashSet<String> {
    CALLBACK_SERVER_TOOLS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

fn generate_runtime(
    ctx: &RunContext,
    cwd: &str,
    mcp_secret_b64: &str,
    allowed_tools: &HashSet<String>,
    port: u16,
) -> String {
    // Intersect agent's allowed tools with what the callback server actually supports
    let server_tools: HashSet<String> = CALLBACK_SERVER_TOOLS
        .iter()
        .map(|s| s.to_string())
        .collect();
    let normalized = normalize_tool_names(allowed_tools);
    let available: Vec<&str> = server_tools
        .iter()
        .filter(|t| normalized.contains(t.as_str()))
        .map(|s| s.as_str())
        .collect();

    let tool_bindings: Vec<String> = available
        .iter()
        .map(|tool| {
            format!(
                r#"  {tool}: async (params: Record<string, unknown> = {{}}) => callTool("{tool}", params)"#,
                tool = tool
            )
        })
        .collect();

    // Escape cwd for embedding in JS string
    let cwd_escaped = cwd.replace('\\', "\\\\").replace('"', "\\\"");
    let run_id_js = format!("\"{}\"", ctx.run_id);
    let project_key_js = format!("\"{}\"", ctx.project_key);

    format!(
        r#"// Cairn Execute Runtime
// Project: {project_id}
// CWD: {cwd}

const CALLBACK_URL = "http://127.0.0.1:{port}/api/mcp";
const MCP_SECRET_B64 = "{mcp_secret_b64}";
const TOTP_TIME_STEP = {totp_step};
const CWD = "{cwd_escaped}";
const RUN_ID: string | null = {run_id_js};
const PROJECT_ID: string | null = {project_key_js};

// TOTP passcode generation (HMAC-SHA256, matches Rust auth.rs)
async function generatePasscode(): Promise<string> {{
  const keyData = Uint8Array.from(atob(MCP_SECRET_B64), c => c.charCodeAt(0));
  const key = await crypto.subtle.importKey("raw", keyData, {{ name: "HMAC", hash: "SHA-256" }}, false, ["sign"]);
  const timeStep = BigInt(Math.floor(Date.now() / 1000 / TOTP_TIME_STEP));
  const buf = new ArrayBuffer(8);
  new DataView(buf).setBigUint64(0, timeStep);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", key, buf));
  return Array.from(sig.slice(0, 8)).map(b => b.toString(16).padStart(2, "0")).join("");
}}

interface McpCallbackResponse {{
  result: string;
}}

async function callTool(tool: string, params: Record<string, unknown>): Promise<McpCallbackResponse> {{
  const passcode = await generatePasscode();
  const response = await fetch(CALLBACK_URL, {{
    method: "POST",
    headers: {{
      "Content-Type": "application/json",
      "Authorization": `Bearer ${{passcode}}`
    }},
    body: JSON.stringify({{ cwd: CWD, run_id: RUN_ID, tool, payload: params }})
  }});
  
  if (!response.ok) {{
    throw new Error(`MCP call failed: ${{response.status}} ${{response.statusText}}`);
  }}
  
  return response.json();
}}

// Tool bindings
const mcp = {{
{tool_bindings}
}};

// Smart read: routes cairn:// URIs to read_issue_resource, files to read
async function read(pathOrParams: string | Record<string, unknown>): Promise<McpCallbackResponse> {{
  const params = typeof pathOrParams === "string" ? {{ path: pathOrParams }} : pathOrParams;
  const path = params.path as string;
  if (path && path.startsWith("cairn://")) {{
    return callTool("read_issue_resource", {{ uri: path }});
  }}
  return callTool("read", params);
}}

// Override mcp.read with smart version
mcp.read = read;

// Convenience exports
const write = mcp.write;
const edit = mcp.edit;
const bash = mcp.bash;
"#,
        project_id = ctx.project_id,
        cwd = cwd,
        port = port,
        mcp_secret_b64 = mcp_secret_b64,
        totp_step = TOTP_TIME_STEP_SECS,
        cwd_escaped = cwd_escaped,
        run_id_js = run_id_js,
        project_key_js = project_key_js,
        tool_bindings = tool_bindings.join(",\n"),
    )
}

pub(crate) fn execute_code(
    ctx: &RunContext,
    cwd: &str,
    payload: &ExecutePayload,
    mcp_secret: &str,
    allowed_tools: &HashSet<String>,
    port: u16,
) -> Result<ExecuteResult, String> {
    let runtime = generate_runtime(ctx, cwd, mcp_secret, allowed_tools, port);

    let mut temp_file = NamedTempFile::with_suffix(".ts")
        .map_err(|e| format!("Failed to create temp file: {}", e))?;

    // Create a temp file for capturing the return value
    let result_file =
        NamedTempFile::new().map_err(|e| format!("Failed to create result file: {}", e))?;
    let result_path = result_file.path().to_string_lossy().to_string();

    // Wrap user code in async IIFE so `return` works naturally
    let full_code = format!(
        r#"{runtime}

// User code (wrapped in async IIFE for return support)
const __cairnResult = await (async () => {{
{user_code}
}})();

// Write return value to result file if defined
if (__cairnResult !== undefined) {{
  const __s = typeof __cairnResult === 'string'
    ? __cairnResult
    : JSON.stringify(__cairnResult, null, 2);
  await Bun.write(Bun.env.__CAIRN_RESULT_FILE!, __s);
}}"#,
        runtime = runtime,
        user_code = payload.code,
    );

    temp_file
        .write_all(full_code.as_bytes())
        .map_err(|e| format!("Failed to write code: {}", e))?;

    let temp_path = temp_file.path().to_string_lossy().to_string();

    let bun_path = crate::env::find_binary("bun").map_err(|e| format!("Bun not found: {}", e))?;

    let timeout_secs = payload.timeout.unwrap_or(300).min(600);

    let mut child = std::process::Command::new(&bun_path)
        .args(["run", &temp_path])
        .current_dir(cwd)
        .env("PATH", crate::env::get_user_path())
        .env("__CAIRN_RESULT_FILE", &result_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to spawn bun: {}", e))?;

    let timeout = std::time::Duration::from_secs(timeout_secs as u64);
    let start = std::time::Instant::now();

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let stdout = child
                    .stdout
                    .take()
                    .map(|mut s| {
                        let mut buf = String::new();
                        std::io::Read::read_to_string(&mut s, &mut buf).ok();
                        buf
                    })
                    .unwrap_or_default();

                let stderr = child
                    .stderr
                    .take()
                    .map(|mut s| {
                        let mut buf = String::new();
                        std::io::Read::read_to_string(&mut s, &mut buf).ok();
                        buf
                    })
                    .unwrap_or_default();

                // Read captured return value from result file
                let result = std::fs::read_to_string(&result_path)
                    .ok()
                    .filter(|s| !s.is_empty());

                return Ok(ExecuteResult {
                    result,
                    stdout,
                    stderr,
                    exit_code: status.code(),
                });
            }
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    return Ok(ExecuteResult {
                        result: None,
                        stdout: String::new(),
                        stderr: format!("Execution timed out after {} seconds", timeout_secs),
                        exit_code: None,
                    });
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(e) => {
                return Err(format!("Failed to wait for bun: {}", e));
            }
        }
    }
}

pub(crate) fn get_allowed_tools_for_run(
    conn: &mut diesel::SqliteConnection,
    ctx: &RunContext,
    config_dir: &Path,
) -> HashSet<String> {
    let job_data: Option<(Option<String>, Option<String>)> = jobs::table
        .find(&ctx.job_id)
        .select((jobs::agent_config_id, jobs::execution_id))
        .first(conn)
        .ok();

    let (agent_config_id, execution_id) = match job_data {
        Some((Some(aid), exec_id)) => (aid, exec_id),
        _ => return get_default_tools(),
    };

    let agent_config: Option<AgentConfig> = if let Some(exec_id) = execution_id {
        load_agent_from_snapshot(conn, &exec_id, &agent_config_id)
            .ok()
            .flatten()
    } else {
        None
    };

    let agent_config = agent_config.or_else(|| {
        let project_path: Option<PathBuf> = projects::table
            .find(&ctx.project_id)
            .select(projects::repo_path)
            .first::<String>(conn)
            .ok()
            .map(PathBuf::from);

        config_agents::get_agent(config_dir, &agent_config_id, project_path.as_deref())
            .ok()
            .flatten()
            .map(|fa| AgentConfig {
                id: fa.id,
                name: fa.name,
                description: fa.description,
                prompt: fa.prompt,
                tools: fa.tools,
                model: fa.model,
                workspace_id: None,
                project_id: None,
                created_at: 0,
                updated_at: 0,
                disallowed_tools: fa.disallowed_tools,
                skills: fa.skills,
                permission_mode: fa.permission_mode,
            })
    });

    match agent_config {
        Some(config) => {
            let mut allowed: HashSet<String> = config.tools.into_iter().collect();
            if let Some(disallowed) = config.disallowed_tools {
                for tool in disallowed {
                    allowed.remove(&tool);
                }
            }
            allowed
        }
        None => get_default_tools(),
    }
}

fn load_agent_from_snapshot(
    conn: &mut diesel::SqliteConnection,
    execution_id: &str,
    agent_config_id: &str,
) -> Result<Option<AgentConfig>, String> {
    use crate::models::ExecutionSnapshot;
    use crate::schema::executions;

    let snapshot_json: Option<String> = executions::table
        .find(execution_id)
        .select(executions::snapshot)
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to load execution: {}", e))?
        .flatten();

    let Some(json) = snapshot_json else {
        return Ok(None);
    };

    let snapshot: ExecutionSnapshot =
        serde_json::from_str(&json).map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    snapshot
        .agents
        .get(agent_config_id)
        .map(|agent| {
            Ok(AgentConfig {
                id: agent.id.clone(),
                name: agent.name.clone(),
                description: agent.description.clone(),
                prompt: agent.prompt.clone(),
                tools: agent.tools.clone(),
                model: agent.model.clone(),
                workspace_id: None,
                project_id: None,
                created_at: snapshot.created_at as i32,
                updated_at: snapshot.created_at as i32,
                disallowed_tools: agent.disallowed_tools.clone(),
                skills: agent.skills.clone(),
                permission_mode: agent.permission_mode.clone(),
            })
        })
        .transpose()
}

pub(crate) fn normalize_tool_names(tools: &HashSet<String>) -> HashSet<String> {
    let mut normalized = HashSet::new();
    for tool in tools {
        normalized.insert(tool.clone());
        normalized.insert(tool.to_lowercase());
        if tool.starts_with("mcp__cairn__") {
            normalized.insert(tool.strip_prefix("mcp__cairn__").unwrap().to_string());
        }
    }
    normalized
}

/// Format an ExecuteResult into a ToolResponse JSON string.
/// Uses return value as primary output, falls back to stdout.
/// Stderr is always appended if present.
pub(crate) fn format_tool_response(result: &ExecuteResult) -> String {
    let is_error = result.exit_code.is_some_and(|c| c != 0);

    // Primary output: return value if present, else stdout
    let primary = result
        .result
        .as_deref()
        .unwrap_or(&result.stdout)
        .trim_end();

    let stderr = result.stderr.trim_end();

    let output = if stderr.is_empty() {
        primary.to_string()
    } else if primary.is_empty() {
        format!("stderr:\n{}", stderr)
    } else {
        format!("{}\n\nstderr:\n{}", primary, stderr)
    };

    serde_json::to_string(&ToolResponse { output, is_error })
        .unwrap_or_else(|_| r#"{"output":"execution error","isError":true}"#.to_string())
}

pub(crate) fn format_error(message: &str) -> String {
    serde_json::to_string(&ToolResponse {
        output: message.to_string(),
        is_error: true,
    })
    .unwrap_or_else(|_| format!(r#"{{"output":"{}","isError":true}}"#, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_tool_names() {
        let mut tools = HashSet::new();
        tools.insert("Read".to_string());
        tools.insert("mcp__cairn__bash".to_string());

        let normalized = normalize_tool_names(&tools);

        assert!(normalized.contains("Read"));
        assert!(normalized.contains("read"));
        assert!(normalized.contains("mcp__cairn__bash"));
        assert!(normalized.contains("bash"));
    }

    #[test]
    fn test_get_default_tools() {
        let tools = get_default_tools();
        assert!(tools.contains("read"));
        assert!(tools.contains("write"));
        assert!(tools.contains("bash"));
    }

    #[test]
    fn test_format_error() {
        let result = format_error("Test error");
        let parsed: ToolResponse = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.output, "Test error");
        assert!(parsed.is_error);
    }

    #[test]
    fn test_format_tool_response_with_return_value() {
        let result = ExecuteResult {
            result: Some("hello world".to_string()),
            stdout: "ignored stdout".to_string(),
            stderr: String::new(),
            exit_code: Some(0),
        };
        let parsed: ToolResponse = serde_json::from_str(&format_tool_response(&result)).unwrap();
        assert_eq!(parsed.output, "hello world");
        assert!(!parsed.is_error);
    }

    #[test]
    fn test_format_tool_response_falls_back_to_stdout() {
        let result = ExecuteResult {
            result: None,
            stdout: "stdout output\n".to_string(),
            stderr: String::new(),
            exit_code: Some(0),
        };
        let parsed: ToolResponse = serde_json::from_str(&format_tool_response(&result)).unwrap();
        assert_eq!(parsed.output, "stdout output");
        assert!(!parsed.is_error);
    }

    #[test]
    fn test_format_tool_response_includes_stderr() {
        let result = ExecuteResult {
            result: Some("result".to_string()),
            stdout: String::new(),
            stderr: "warning here\n".to_string(),
            exit_code: Some(0),
        };
        let parsed: ToolResponse = serde_json::from_str(&format_tool_response(&result)).unwrap();
        assert_eq!(parsed.output, "result\n\nstderr:\nwarning here");
        assert!(!parsed.is_error);
    }

    #[test]
    fn test_format_tool_response_error_exit_code() {
        let result = ExecuteResult {
            result: None,
            stdout: String::new(),
            stderr: "ReferenceError: x is not defined".to_string(),
            exit_code: Some(1),
        };
        let parsed: ToolResponse = serde_json::from_str(&format_tool_response(&result)).unwrap();
        assert!(parsed.is_error);
        assert!(parsed.output.contains("ReferenceError"));
    }
}
