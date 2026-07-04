//! Real-anatomy event-`data` builders shared by every archival test.
//!
//! Archival classification (`crate::archival::rewrite`) and reconstruction
//! ([`super::reconstruct`]) read the exact JSON the backends record into
//! `events.data` (a camelCase `TranscriptEvent`). Three early bugs came from
//! tests hand-rolling a shape the backends never emit, so every archival test now
//! builds its events here and a wrong-shape assumption fails in one place.
//!
//! The load-bearing anatomy, derived from `agent_process::stream` (the Claude
//! reader) and `backends::codex::events` (the Codex reader), both of which
//! serialize a `TranscriptEvent`:
//! - an **assistant** tool call carries `toolUses: [{id, name, input}]`. Real
//!   sessions record MCP-prefixed names — the Claude CLI emits `mcp__cairn__read`
//!   and Codex builds `format!("mcp__{server}__{tool}")` — so the classifier must
//!   normalize them; these builders use the prefixed form.
//! - the paired **tool_result** carries `toolUseId` + a string `toolResult` and
//!   **no** `toolInput`: a read's paths are recovered by pairing back to the
//!   call, never from the result. Claude *also* records the rendered result a
//!   second time under `raw.tool_use_result` (content blocks whose `text`
//!   duplicates `toolResult` byte for byte); archival must drop this duplicate,
//!   so these builders carry it. Codex never duplicates here (its reader keeps
//!   `raw` only for non-text MCP content, which a read never produces).
//! - a **write** result's `toolResult` is the short change-report JSON; the heavy
//!   change payload lives on the *assistant* event's `toolUses[].input`, not on
//!   the result. Archival keys off `toolUses` (the authoritative location), so
//!   these builders carry the call's input there.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// An assistant event issuing a single tool call with the given `name`/`input`.
fn assistant_call(id: &str, name: &str, input: Value) -> String {
    json!({
        "eventType": "assistant",
        "toolUses": [{ "id": id, "name": name, "input": input }]
    })
    .to_string()
}

/// Assistant issuing an MCP read of `paths`.
pub fn assistant_read(id: &str, paths: &[&str]) -> String {
    assistant_call(id, "mcp__cairn__read", json!({ "paths": paths }))
}

/// Assistant issuing an MCP write of two file changes whose heavy payloads
/// (`HEAVYPAYLOAD*`) are exactly what archival must strip. The targets match the
/// W1 commit the rewrite/reconstruct fixtures build (a.txt patched, c.txt added),
/// so the regenerated per-change diff reproduces `git show W1`.
pub fn assistant_write(id: &str) -> String {
    assistant_call(
        id,
        "mcp__cairn__write",
        json!({
            "changes": [
                { "target": "file:a.txt", "mode": "patch",
                  "payload": { "old_string": "alpha", "new_string": "HEAVYPAYLOADALPHA" } },
                { "target": "file:c.txt", "mode": "create",
                  "payload": { "content": "HEAVYPAYLOADNEWFILE\n" } }
            ],
            "commit_msg": "w"
        }),
    )
}

/// The payload-stripped remainder of [`assistant_write`]: the call skeleton kept
/// after archival drops every `changes[*].payload`, as stored (zstd) in
/// `data_blob`. Reconstruction re-injects the committed diff into each change.
pub fn assistant_write_remainder(id: &str) -> String {
    assistant_call(
        id,
        "mcp__cairn__write",
        json!({
            "changes": [
                { "target": "file:a.txt", "mode": "patch" },
                { "target": "file:c.txt", "mode": "create" }
            ],
            "commit_msg": "w"
        }),
    )
}

/// A single-tool-use MCP write exactly as the backends serialize it: the call
/// input appears BOTH under `toolUses[0].input` and, for backwards compatibility,
/// a duplicate top-level `toolInput` (`agent_process::stream` mirrors a lone
/// tool call's input there). Archival must strip the heavy `HEAVYPAYLOAD*` bytes
/// from both copies. The targets match the W1 commit (a.txt patched, c.txt
/// added), so the regenerated per-change diff reproduces `git show W1`.
pub fn assistant_write_dup_tool_input(id: &str) -> String {
    let input = json!({
        "changes": [
            { "target": "file:a.txt", "mode": "patch",
              "payload": { "old_string": "alpha", "new_string": "HEAVYPAYLOADALPHA" } },
            { "target": "file:c.txt", "mode": "create",
              "payload": { "content": "HEAVYPAYLOADNEWFILE\n" } }
        ],
        "commit_msg": "w"
    });
    json!({
        "eventType": "assistant",
        "toolName": "mcp__cairn__write",
        "toolInput": input,
        "toolUses": [{ "id": id, "name": "mcp__cairn__write", "input": input }]
    })
    .to_string()
}

/// Assistant issuing an MCP run (a command that commits out of band).
pub fn assistant_run(id: &str) -> String {
    assistant_call(
        id,
        "mcp__cairn__run",
        json!({ "commands": [{ "command": "git commit -am drift" }] }),
    )
}

/// Assistant issuing a tool call with an explicit `name`, so a test can pin the
/// exact MCP-prefix variant (`mcp__cairn__write`, the dotted `mcp__cairn.read`,
/// a non-MCP name, …).
pub fn assistant_tool(id: &str, name: &str, input: Value) -> String {
    assistant_call(id, name, input)
}

/// Assistant model text (no tool call).
pub fn assistant_text(content: &str) -> String {
    json!({ "eventType": "assistant", "content": content }).to_string()
}

/// Assistant extended-thinking block.
pub fn assistant_thinking(thinking: &str) -> String {
    json!({ "eventType": "assistant", "thinking": thinking }).to_string()
}

/// A user text event.
pub fn user_text(content: &str) -> String {
    json!({ "eventType": "user", "content": content }).to_string()
}

/// A backend system event (`system:<subtype>`, e.g. `system:init`).
pub fn system_event(subtype: &str) -> String {
    json!({ "eventType": format!("system:{subtype}") }).to_string()
}

/// A Claude `system:init` handshake exactly as `agent_process::stream` records it:
/// the `TranscriptEvent` top-level in struct-declaration order, with the CLI's
/// full local inventory under `raw` (a `serde_json::Value`, so its keys serialize
/// alphabetically). `session_id`/`uuid`/`cwd` and the `tools` order are the only
/// per-run-varying parts; everything else is the near-constant machine config the
/// archival path content-addresses once. `tools` order is taken as given so a
/// test can reproduce the CLI's per-run shuffle.
pub fn system_init_claude(session_id: &str, uuid: &str, cwd: &str, tools: &[&str]) -> String {
    // Built via `json!` (a BTreeMap when serde_json has no preserve_order, as in
    // this workspace) so `raw` is alphabetical, matching the recorder. The top
    // level is assembled by hand because struct field order is not alphabetical.
    let raw = json!({
        "agents": ["general-purpose", "Explore", "Plan"],
        "apiKeySource": "none",
        "claude_code_version": "2.0.76",
        "cwd": cwd,
        "mcp_servers": [{ "name": "cairn", "status": "connected" }],
        "model": "claude-sonnet-4-5-20250929",
        "output_style": "default",
        "permissionMode": "default",
        "plugins": [{
            "name": "swift-lsp",
            "path": "/Users/x/.claude/plugins/cache/claude-plugins-official/swift-lsp/1.0.0"
        }],
        "session_id": session_id,
        "skills": ["frontend-design:frontend-design"],
        "slash_commands": ["compact", "context", "review"],
        "subtype": "init",
        "tools": tools,
        "type": "system",
        "uuid": uuid,
    });
    format!(
        concat!(
            "{{\"eventType\":\"system:init\",\"sessionId\":{},\"parentToolUseId\":null,",
            "\"content\":null,\"thinking\":null,\"toolName\":null,\"toolInput\":null,",
            "\"toolUses\":null,\"toolUseId\":null,\"toolResult\":null,\"isError\":false,",
            "\"raw\":{}}}"
        ),
        Value::String(session_id.to_string()),
        raw,
    )
}

/// A Codex `system:init` exactly as `backends::codex::runtime` records it: a fixed
/// struct literal where only `sessionId` varies (`content` is constant and `raw`
/// is absent). The degenerate case of the same archival path — one shared
/// skeleton, no tool set.
pub fn system_init_codex(session_id: &str) -> String {
    format!(
        concat!(
            "{{\"eventType\":\"system:init\",\"sessionId\":{},\"parentToolUseId\":null,",
            "\"content\":\"Codex session started\",\"thinking\":null,\"toolName\":null,",
            "\"toolInput\":null,\"toolUses\":null,\"toolUseId\":null,\"toolResult\":null,",
            "\"isError\":false,\"raw\":null}}"
        ),
        Value::String(session_id.to_string()),
    )
}

/// A `system:prompt` event's `data` exactly as `persist_system_prompt_event`
/// records it: the concatenated `content`, plus `raw.hash` and a `raw.segments`
/// boundary map of `{kind, byteOffset, byteLen}`. Every kind but `dynamic` is
/// static across runs and content-addresses once into `archival_blobs`; the
/// `dynamic` tail inlines. Returns `(data, full_content)`. Shared by the archival
/// rewrite and backfill tests so both writers exercise the identical anatomy.
pub fn system_prompt(segments: &[(&str, &str)]) -> (String, String) {
    let mut content = String::new();
    let mut seg_map = Vec::new();
    for (kind, text) in segments {
        let offset = content.len();
        content.push_str(text);
        seg_map.push(json!({ "kind": kind, "byteOffset": offset, "byteLen": text.len() }));
    }
    let data = json!({
        "eventType": "system:prompt",
        "content": content,
        "raw": {
            "backend": "claude",
            "bytes": content.len(),
            "hash": format!("{:x}", Sha256::digest(content.as_bytes())),
            "segments": seg_map,
        },
    })
    .to_string();
    (data, content)
}

/// A tool_result paired to a call: `toolUseId` + a string `toolResult`, no
/// `toolInput`. Used for read results, run output, and any non-write result.
///
/// Carries Claude's `raw.tool_use_result` duplicate of the rendered result (see
/// the module docs): archival must strip it from gitcoord-read stubs, so the
/// fixture keeps the tests honest about real recorded anatomy.
pub fn tool_result(id: &str, result: &str) -> String {
    json!({
        "eventType": "tool_result",
        "toolUseId": id,
        "toolResult": result,
        "raw": {
            "type": "user",
            "message": { "role": "user" },
            "session_id": "sess-fixture",
            "tool_use_result": [{ "type": "text", "text": result }],
        },
    })
    .to_string()
}

/// The short change-report a committed `write` returns as its `toolResult`.
pub fn change_report(sha: &str) -> String {
    json!({
        "applied": [{ "index": 0, "target": "file:a.txt", "mode": "patch",
            "kind": "file", "summary": "patched a.txt" }],
        "commit": { "status": "committed", "sha": sha, "prNumber": null, "message": null }
    })
    .to_string()
}

/// A committed write's `tool_result`: `toolUseId` + the short change-report.
pub fn write_result(id: &str, sha: &str) -> String {
    tool_result(id, &change_report(sha))
}

/// The archived gitcoord-read stub stored in `data`: the heavy `toolResult` is
/// dropped and `toolInput.paths` is pinned — the contract reconstruction
/// dispatches on. Mirrors what the writer's `read_stub` persists.
pub fn read_stub(id: &str, paths: &[&str]) -> String {
    json!({
        "eventType": "tool_result",
        "toolUseId": id,
        "toolName": "read",
        "toolInput": { "paths": paths },
        "toolResult": Value::Null,
    })
    .to_string()
}

/// Render targets exactly as a live read did (and as reconstruction will), so a
/// stored `toolResult` is byte-identical to what the agent saw. Routes through
/// the shared segment builder and the shared `assemble` — the genuine live
/// composition, not an imitation — so a per-shape round-trip test can never drift
/// from the producer it stands in for. End-to-end equality with a real
/// `handle_read_batch` is asserted separately (see the rewrite tests'
/// `live_read_batch_round_trips_through_archival`); this builder is the
/// convenience the per-shape tests assert against.
pub fn render_targets(targets: &[(&str, &[u8])]) -> String {
    let segments: Vec<cairn_common::read::ReadSegment> = targets
        .iter()
        .map(|(target, bytes)| {
            crate::mcp::handlers::read::produce_archived_file_segment(target, bytes)
                .expect("produce archived file segment")
        })
        .collect();
    crate::mcp::handlers::read::view::assemble(segments).text
}

/// One section of a mixed read batch for the hybrid-archival fixtures: a `file:`
/// target (rebuilt from git on reconstruction) or a verbatim resource section
/// (stored in the skeleton).
pub enum MixedSection<'a> {
    File(&'a str, &'a [u8]),
    Resource(&'a str, &'a str),
}

/// Compose a mixed read batch exactly as the live `read_batch` did (and as
/// reconstruction will): file sections route through the shared segment builder,
/// resource sections stay verbatim. Mirrors [`render_targets`] for the per-target
/// hybrid path, where only the `file:` sections are git-addressed.
pub fn mixed_render_targets(sections: &[MixedSection]) -> String {
    use cairn_common::read::{NaturalUnit, ReadSegment, SegmentKind, SegmentMeta};
    let segments: Vec<ReadSegment> = sections
        .iter()
        .map(|section| match section {
            MixedSection::File(target, bytes) => {
                crate::mcp::handlers::read::produce_archived_file_segment(target, bytes)
                    .expect("produce archived file segment")
            }
            MixedSection::Resource(uri, body) => ReadSegment::text(
                *body,
                SegmentMeta::new(*uri, SegmentKind::Resource, NaturalUnit::Line),
            ),
        })
        .collect();
    crate::mcp::handlers::read::view::assemble(segments).text
}

/// The archived hybrid-read stub stored in `data`: the heavy `toolResult` is
/// dropped, `toolInput.paths` is pinned, plus the `hybrid_read` marker and the
/// indices of the git-addressed file sections. Mirrors what the writer's
/// `hybrid_stub` persists.
pub fn hybrid_read_stub(id: &str, paths: &[&str], indices: &[usize]) -> String {
    json!({
        "eventType": "tool_result",
        "toolUseId": id,
        "toolName": "read",
        "toolInput": { "paths": paths },
        "toolResult": Value::Null,
        "archived": "hybrid_read",
        "sections": indices,
    })
    .to_string()
}
