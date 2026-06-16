//! Toolkit and MCP server types.

use serde::{Deserialize, Serialize};

/// Tools that are always disallowed for every agent, regardless of config.
///
/// Two categories live here.
///
/// **Cairn-managed equivalents** — a Cairn verb already owns this job, so the
/// native version must never run:
/// - `EnterPlanMode`/`ExitPlanMode`: planning mode is managed by Cairn.
/// - `TodoWrite`: native todo writes are replaced by the job's todos URI via the
///   `write` tool. Leaving native `TodoWrite` enabled would silently store
///   nothing, so it must stay disallowed for every agent.
///
/// **Host-harness built-ins** — Claude Code (the host harness) keeps adding
/// built-in tools that are *declared to the model* unless they are named in
/// `--disallowedTools`. None has a Cairn equivalent: all work flows through the
/// three verbs (`read`/`write`/`run`), so every one of these is confusing
/// surface area that must never be offered (e.g. an agent reaching for `Monitor`
/// to watch a process or `ScheduleWakeup` to pace work instead of using `run`).
///
/// There is no deny-by-default lever in the CLI: `--allowedTools` governs
/// *permission* (which tools run without a prompt), not which tools are
/// *declared*, so allow-listing the three verbs does not hide anything. The
/// only way to remove a built-in from the declared surface is to name it here.
/// That means this list must be kept current — when a CLI update adds a new
/// built-in, add it here or it will leak into agent sessions. Observed set as of
/// Claude Code (2026-06):
pub const ALWAYS_DISALLOWED_TOOLS: &[&str] = &[
    // Cairn-managed equivalents
    "EnterPlanMode",
    "ExitPlanMode",
    "TodoWrite",
    // Host-harness built-ins (Claude Code) — keep current with each CLI update.
    "CronCreate",
    "CronDelete",
    "CronList",
    "ScheduleWakeup",
    "RemoteTrigger",
    "EnterWorktree",
    "ExitWorktree",
    "ListMcpResourcesTool",
    "ReadMcpResourceTool",
    "Monitor",
    "TaskStop",
    "PushNotification",
    "DesignSync",
    "Workflow",
];

/// Every native provider tool, all of which are hard-disabled.
///
/// Cairn exposes exactly three working verbs — `read`, `write`, `run` (plus
/// the corpus tools: `create_pr`, `return`, memory tools, read-only issue/plan
/// access). Native provider tools never run: `Read`/`Write`/`Edit`/`Bash` are
/// aliased to the Cairn verbs in `agent_process::toolkits`, and the rest have
/// no Cairn equivalent (skills arrive via `cairn://skills` + the slash-command
/// hook; web/PDF reads route through `read` → bmd; sub-agents and user
/// questions go through `write` appends to the node's collections).
///
/// This is the single source for Claude's `--disallowedTools`. Codex ignores
/// disallow lists, so native-off there is enforced by simply not allowing them.
pub const ALL_NATIVE_TOOLS: &[&str] = &[
    "Read",
    "Write",
    "Edit",
    "Bash",
    "Task",
    "TaskOutput",
    "AskUserQuestion",
    "WebFetch",
    "WebSearch",
    "Glob",
    "Grep",
    "LSP",
    "Skill",
    "NotebookEdit",
];

/// A toolkit override stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Toolkit {
    pub id: String,
    pub stage: String, // "plan", "implementation", "chat"
    pub allowed_tools: Option<Vec<String>>,
    pub disallowed_tools: Option<Vec<String>>,
    pub created_at: i64,
    pub updated_at: i64,
}
