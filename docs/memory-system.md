# Memory System

The memory system lets agents learn from past work and surface relevant knowledge in future sessions. Agents create memories with trigger conditions; when a future tool call matches those conditions, the memory content is injected into the agent's context.

## Data Model

A **memory** has:
- **content** — the knowledge to surface (free text)
- **confidence** — `tentative` or `established`. Tentative memories surface with a qualifier ("A previous agent found: ..."), established memories surface directly
- **project scope** — memories belong to a project (matched by working directory) or are global (project_id = NULL)
- **active flag** — soft delete. Deactivated memories stop matching but remain in the database
- **surfacing stats** — `surfaced_count` and `last_surfaced_at` track how often and how recently a memory has been used
- **source_issue** — optional link back to the issue where the memory was created

A memory has one or more **triggers**. Each trigger specifies:
- **trigger_index** — groups conditions. Conditions with the same index are AND-ed together; different indices are OR-ed
- **json_path** — dot-notation path into the hook's JSON payload (e.g., `tool_name`, `tool_input.file_path`)
- **pattern** — regex matched against the extracted value

### Trigger Logic

Triggers with the same `trigger_index` form a group. All conditions in a group must match (AND). If any group matches completely, the memory matches (OR across groups).

Example: a memory about Diesel migration patterns might have:

```
Group 0: tool_name matches "^edit$"  AND  tool_input.file_path matches "schema\.rs$"
Group 1: tool_name matches "^bash$"  AND  tool_input.command matches "^diesel "
```

This memory surfaces when an agent edits `schema.rs` OR runs a diesel command.

## Surfacing Mechanism

Memories are **not** injected into the system prompt. Instead, they're surfaced dynamically through Claude Code hooks.

### Hook Integration

At session startup, Cairn writes a hook settings file (`~/.cairn/hook-settings.json`) that configures three Claude Code hooks:
- `PostToolUse` — fires after a successful tool call
- `PostToolUseFailure` — fires after a failed tool call  
- `UserPromptSubmit` — fires when a user message is submitted

Each hook runs a curl command that POSTs the hook's stdin JSON to `http://127.0.0.1:3847/api/memories/match`. The hook settings file is passed to Claude CLI via `--settings`.

### Matching Flow

When a hook fires:

1. **Parse the hook payload** — the JSON includes `tool_name`, `tool_input` (with tool-specific fields like `file_path` or `command`), `tool_result`, `cwd`, `session_id`, and `hook_event_name`.

2. **Resolve the project** — the `cwd` is matched against known project repo paths, worktree paths, and job worktree assignments. This determines which project-scoped memories to consider (plus global memories).

3. **Load active memories** — query memories where `active = 1` and either `project_id` matches or `project_id IS NULL`.

4. **Match triggers** — for each memory, group triggers by `trigger_index`. For each group, extract the value at `json_path` from the hook JSON and test it against the `pattern` regex. If all conditions in any group match, the memory matches.

5. **Record surfacing** — increment `surfaced_count` and update `last_surfaced_at` for matched memories.

6. **Insert event** — a `system:memory` event is inserted into the run's event stream, recording what was surfaced and why (the triggering tool and context).

7. **Return context** — the matched memory content is returned as `additionalContext` in the hook response:

   ```json
   {
     "hookSpecificOutput": {
       "hookEventName": "PostToolUse",
       "additionalContext": "[Memory] Content of matched memory\n[Memory] Another matched memory"
     }
   }
   ```

   Claude sees the `additionalContext` on its next turn, alongside the tool result.

### Message Delivery

The same hook endpoint also handles cross-agent message delivery. When checking for memories, it also polls for new channel messages addressed to the current agent, inserts them as `system:message` events, and includes them in the `additionalContext` response. This piggybacks message delivery on the existing hook infrastructure.

## Agent-Authored Memories

Agents can create, update, and deactivate memories through MCP tools:

- **create_memory** — content, optional confidence (defaults to tentative), triggers with json_path + pattern pairs
- **update_memory** — modify content, confidence, active status, or replace triggers
- **deactivate_memory** — soft-delete (sets active = false)
- **list_memories** — view memories for the current project, optionally including inactive ones

A typical workflow: a Reviewer agent analyzes a completed job, identifies a pattern worth remembering, and creates a tentative memory with triggers matching the relevant file paths or tool patterns. If the memory proves useful (surfaces correctly, isn't contradicted), it can be promoted to established confidence.

## Confidence Levels

**Tentative** memories surface with framing: "*A previous agent found: {content}*". This signals to the receiving agent that the information is provisional — it came from a single observation and may not generalize.

**Established** memories surface directly as the content string. These represent validated knowledge that the system is confident about.

The distinction lets agents treat memories with appropriate skepticism. A tentative memory about a quirky API behavior is useful context but shouldn't override documentation; an established memory about a project convention should be followed.

## JSONPath Extraction

The `json_path` field uses dot-notation to navigate the hook JSON:

- `tool_name` → `json["tool_name"]`
- `tool_input.file_path` → `json["tool_input"]["file_path"]`
- `tool_input.command` → `json["tool_input"]["command"]`

A leading `$.` prefix is stripped if present. The extracted value is converted to a string for regex matching — strings are used directly, numbers and booleans are stringified, null values cause the condition to fail.
