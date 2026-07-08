//! Batch read assembler and the `read_batch` dispatch entry.
//!
//! The CLI resolves the `paths` array and forwards it as one `read_batch`
//! callback. Here we classify each target, run its producer to a
//! [`ReadSegment`], window/truncate each under one shared budget via the view
//! layer, re-apply per-target flail dedup (CAIRN-1271), append deduped
//! affordances once after truncation, and return a [`ReadBatchEnvelope`].

use std::collections::HashMap;
use std::sync::Mutex;

use cairn_common::query::{encode_query_params, split_target_query, QueryParam};
use cairn_common::read::{NaturalUnit, ReadSegment, SegmentKind, SegmentMeta};
use cairn_common::uri::{parse_uri, CairnResource};
use serde::Deserialize;

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

use super::{error_segment, view, Produced};

type ReadCursorState = Mutex<HashMap<String, usize>>;

#[derive(Debug, Deserialize)]
struct ReadBatchPayload {
    #[serde(default)]
    paths: Vec<String>,
}

/// Handle a `read_batch` callback: classify, produce, dedup, and assemble.
pub async fn handle_read_batch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    read_cursors: &ReadCursorState,
) -> String {
    let payload: ReadBatchPayload = match crate::mcp::handlers::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };
    if payload.paths.is_empty() {
        return "read_batch requires a non-empty `paths` array.".to_string();
    }

    // Produce every segment first. A fence suspension aborts the whole batch
    // before any dedup is recorded, so the permission flow's re-dispatch re-runs
    // the batch without the earlier targets being mistaken for duplicate calls.
    let mut segments: Vec<ReadSegment> = Vec::with_capacity(payload.paths.len());
    for path in &payload.paths {
        match produce_segment(orch, request, read_cursors, path).await {
            Produced::Segment(segment) => segments.push(segment),
            Produced::Suspended(message) => return message,
        }
    }

    // Per-target flail dedup: re-applied here because `read_batch` is kept out of
    // the outer `is_read_family` so the batch granularity does not collapse the
    // per-target `[duplicate call]` stub.
    if let Some(run_id) = request.run_id.as_deref() {
        for segment in &mut segments {
            // Browser reads are snapshots of an interactive surface; repeating the
            // same URI is an intentional poll/visual re-check, not flailing over
            // a static resource. Always return the fresh browser read result.
            if is_browser_resource_target(&segment.meta.uri) {
                continue;
            }
            // A `?wait` long-poll is an intentional repeated read of the same
            // call URI until it resolves; consecutive pending results are
            // identical by design, so it must bypass flail dedup.
            if is_wait_poll_target(&segment.meta.uri) {
                continue;
            }
            let fingerprint = format!("read_batch_target\u{1}{}", segment.meta.uri);
            let hash = crate::dispatch::hash_content(&segment.body);
            if let crate::agent_process::process::DedupOutcome::Duplicate {
                count,
                distinct_dupes,
            } = orch
                .process_state
                .check_and_record_content(run_id, &fingerprint, hash)
            {
                let stub = crate::dispatch::build_dup_stub(
                    "read",
                    &segment.meta.uri,
                    count,
                    distinct_dupes,
                );
                let uri = segment.meta.uri.clone();
                *segment = error_segment(uri, stub);
            }
        }
    }

    // Session-scoped affordance collapse (CAIRN-2592): the full affordance block
    // for a resource kind renders once per session. A later read whose block was
    // already shown collapses to a one-line `cairn://help?kind=<slug>` pointer,
    // keeping the full reference one cheap read away without re-paying the whole
    // block. Runs on the post-flail segment list (error stubs carry no
    // affordance, so the two passes never fight). Keyed by block-content hash so
    // schema-distinct artifact blocks never collapse into one another. Skipped
    // without a run id (tests / external runs keep full blocks), matching the
    // flail-dedup gate above.
    if let Some(run_id) = request.run_id.as_deref() {
        // Distinct affordance blocks still present. Deciding per distinct hash
        // (not per segment) keeps a kind uniformly full-or-pointer within one
        // batch, so `assemble`'s batch-local `(kind, block)` dedup never emits a
        // full block and a pointer for the same kind in the same call.
        let mut hashes: Vec<u64> = Vec::new();
        for segment in &segments {
            if let Some(affordance) = &segment.affordance {
                let hash = crate::dispatch::hash_content(&affordance.block);
                if !hashes.contains(&hash) {
                    hashes.push(hash);
                }
            }
        }
        if !hashes.is_empty() {
            let already_seen = orch.process_state.mark_affordances_seen(run_id, &hashes);
            for segment in &mut segments {
                if let Some(affordance) = &mut segment.affordance {
                    let hash = crate::dispatch::hash_content(&affordance.block);
                    // Collapse only blocks that faithfully round-trip through the
                    // help projection; `pointer_affordance_block` returns None for
                    // schema-derived artifact blocks, which stay full.
                    if already_seen.contains(&hash) {
                        if let Some(pointer) =
                            crate::resources::pointer_affordance_block(&affordance.block)
                        {
                            affordance.block = pointer;
                        }
                    }
                }
            }
        }
    }

    let envelope = view::assemble(segments);
    serde_json::to_string(&envelope)
        .unwrap_or_else(|error| format!("Failed to serialize read batch: {error}"))
}

/// Classify one resolved target and run its producer.
async fn produce_segment(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    read_cursors: &ReadCursorState,
    target: &str,
) -> Produced {
    if is_web_target(target) {
        return Produced::Segment(produce_web_segment(orch, request, target).await);
    }
    if is_cairn_resource_target(target) {
        return Produced::Segment(
            produce_resource_segment(orch, request, read_cursors, target).await,
        );
    }
    if target.starts_with("file:") {
        let file_payload = crate::mcp::types::ReadFilePayload {
            path: target.to_string(),
            offset: None,
            limit: None,
            issue_history: None,
        };
        return super::produce_file_segment(orch, request, &file_payload).await;
    }
    // Bare worktree-relative fallback (CAIRN-2030, bug #107): a scheme-less
    // target is read as a `file:` target when — with its `?query` split off — it
    // names a path that EXISTS within the worktree. The existence + in-worktree
    // gate is what keeps the leniency safe: clear intent resolves, so a typo or an
    // absolute path outside the worktree still falls through to the error below.
    // Read-only by design — `write` keeps requiring an explicit `file:` so an
    // accidental bare-path write stays hard.
    if let Some(file_target) = bare_worktree_file_target(&request.cwd, target) {
        let file_payload = crate::mcp::types::ReadFilePayload {
            path: file_target,
            offset: None,
            limit: None,
            issue_history: None,
        };
        return super::produce_file_segment(orch, request, &file_payload).await;
    }
    Produced::Segment(error_segment(
        target,
        format!(
            "Invalid target: expected cairn://…, file:…, an http(s) URL, or a local .pdf path, got '{target}'"
        ),
    ))
}

/// Resolve a scheme-less read target against the worktree, returning a
/// `file:`-prefixed target (query preserved) when it names an existing path
/// inside the worktree, else `None` so the caller errors as before.
///
/// The `?query` is split off before the existence check so `docs/x.md?limit=3`
/// tests `docs/x.md` for existence and still carries `limit=3` to the file
/// producer. Acceptance is strictly "exists AND does not escape the worktree":
/// a missing bare path (a typo) or an absolute path resolving outside the
/// worktree returns `None`.
fn bare_worktree_file_target(cwd: &str, target: &str) -> Option<String> {
    let identity = split_target_query(target)
        .map(|split| split.identity)
        .unwrap_or_else(|_| target.to_string());
    if identity.is_empty() {
        return None;
    }
    let worktree = std::path::Path::new(cwd);
    let candidate = worktree.join(&identity);
    if candidate.exists() && !crate::mcp::file_targets::path_escapes_worktree(worktree, &candidate)
    {
        Some(format!("file:{target}"))
    } else {
        None
    }
}

fn is_web_target(target: &str) -> bool {
    if target.starts_with("http://") || target.starts_with("https://") {
        return true;
    }
    let base = split_target_query(target)
        .map(|split| split.identity)
        .unwrap_or_else(|_| target.to_string());
    base.to_lowercase().ends_with(".pdf")
}

fn is_cairn_resource_target(target: &str) -> bool {
    target.starts_with("cairn://") || target == "cairn:~" || target.starts_with("cairn:~/")
}

fn is_browser_resource_target(target: &str) -> bool {
    let Ok(split) = split_target_query(target) else {
        return false;
    };
    matches!(
        parse_uri(&split.identity),
        Some(CairnResource::NodeBrowser { .. })
            | Some(CairnResource::TaskBrowser { .. })
            | Some(CairnResource::ProjectBrowser { .. })
    )
}

/// A `?wait` long-poll on a call/task-artifact URI (the harness `agent()`
/// await). Repeated reads of the same URI are intentional polling, not flailing,
/// so this target bypasses the batch dedup.
fn is_wait_poll_target(target: &str) -> bool {
    let Ok(split) = split_target_query(target) else {
        return false;
    };
    if !split.params.iter().any(|p| p.key == "wait") {
        return false;
    }
    matches!(
        parse_uri(&split.identity),
        Some(CairnResource::TaskArtifact { .. })
    )
}

/// Grep-family params peeled off a materialized body (web URL, terminal buffer)
/// before the body is fetched/forwarded — the grep runs client-side over the
/// rendered text. `offset`/`limit` are stripped by `split_line_scope`; the rest
/// are stripped here.
const GREP_STRIP_KEYS: &[&str] = &[
    "grep",
    "-i",
    "-n",
    "-A",
    "-B",
    "-C",
    "context",
    "head_limit",
    "multiline",
    "output_mode",
];

/// Drop `keys` from a target's query, rebuilding the URL without them. Left
/// byte-for-byte when there is nothing to strip, so plain URLs are untouched.
fn strip_query_keys(target: &str, keys: &[&str]) -> String {
    let split = match split_target_query(target) {
        Ok(split) => split,
        Err(_) => return target.to_string(),
    };
    if !split.params.iter().any(|p| keys.contains(&p.key.as_str())) {
        return target.to_string();
    }
    let remaining: Vec<QueryParam> = split
        .params
        .into_iter()
        .filter(|param| !keys.contains(&param.key.as_str()))
        .collect();
    if remaining.is_empty() {
        split.identity
    } else {
        format!("{}?{}", split.identity, encode_query_params(&remaining))
    }
}

/// A web/PDF segment: fetch markdown via `bmd`, then either grep the markdown
/// (universal grep over the materialized body) or window it client-side, wrapped
/// as a `Grep`/`Web` segment. A bmd failure becomes an inline `Error` segment.
async fn produce_web_segment(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    target: &str,
) -> ReadSegment {
    // Parse grep from the raw target query (web URLs carry no `glob` pushdown).
    let grep_payload = match split_target_query(target) {
        Ok(split) => match crate::mcp::handlers::search::body_grep_payload(&split.params, false) {
            Ok(payload) => payload,
            Err(error) => return error_segment(target, error),
        },
        Err(_) => None,
    };

    let (clean, offset, limit) = split_line_scope(target, true);
    // Peel grep-family params off the URL so they are never forwarded to bmd.
    let clean = strip_query_keys(&clean, GREP_STRIP_KEYS);
    let web_request = McpCallbackRequest {
        thread_id: None,
        cwd: request.cwd.clone(),
        run_id: request.run_id.clone(),
        tool: "read_web".to_string(),
        payload: serde_json::json!({ "target": clean }),
        tool_use_id: request.tool_use_id.clone(),
    };
    match crate::mcp::handlers::web::read_web_markdown(orch, &web_request).await {
        Ok(markdown) => {
            if let Some(payload) = grep_payload {
                let (content, match_count) =
                    crate::mcp::handlers::search::grep_materialized_body(&markdown, &payload);
                let mut meta = SegmentMeta::new(target, SegmentKind::Grep, NaturalUnit::Match);
                meta.match_count = Some(match_count);
                return ReadSegment::text(content, meta);
            }
            let window = view::window_text_lines(&markdown, offset, limit);
            let mut meta = SegmentMeta::new(target, SegmentKind::Web, NaturalUnit::Line);
            meta.total_units = Some(window.total);
            meta.shown_units = window.shown;
            meta.offset = window.offset;
            meta.limit = limit;
            ReadSegment::text(window.body, meta)
        }
        Err(message) => error_segment(target, message),
    }
}

/// A cairn:// resource segment built from the structured resource producer:
/// content and affordance arrive separated, with natural-unit metadata. A
/// `Line` resource is windowed by the shared view layer (offset/limit count
/// lines); a `Record` collection (`/issues`, `/messages`) is already
/// item-windowed by the producer (SQL/cursor pushdown) and must not be
/// re-windowed by lines. Terminals keep their JSON-extraction path.
async fn produce_resource_segment(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    read_cursors: &ReadCursorState,
    target: &str,
) -> ReadSegment {
    let identity = split_target_query(target)
        .map(|split| split.identity)
        .unwrap_or_else(|_| target.to_string());
    if matches!(
        parse_uri(&identity),
        Some(CairnResource::NodeTerminal { .. } | CairnResource::ProjectTerminal { .. })
    ) {
        return produce_terminal_segment(orch, request, read_cursors, target).await;
    }

    // A `cairn:~/terminal/<slug>` from a sub-task agent resolves to a
    // `.../task/{seg}/terminal/{slug}` shape that does not parse as a terminal.
    // Name the constraint instead of failing with a generic unknown-resource error.
    if parse_uri(&identity).is_none() {
        if let Some(hint) = crate::mcp::handlers::resources::task_terminal_hint(&identity) {
            return error_segment(target, hint);
        }
    }

    let rendered = crate::resources::produce_cairn_resource(orch, request, target).await;
    let affordance = rendered.affordance;
    // Image blocks (e.g. a browser screenshot) ride alongside the text body into
    // the segment; `assemble` lifts them into the envelope and cairn-cmd emits
    // each as an MCP image content block.
    let images = rendered.images;

    match rendered.natural_unit {
        NaturalUnit::Match => {
            // Universal grep already paginated the body by head_limit, so the
            // view must not re-window it. Emit a Grep segment; the view renders
            // the `[N matches]` header and the grep continue footer. A single
            // rendered body has no file dimension, so file_count stays None.
            let mut meta = SegmentMeta::new(target, SegmentKind::Grep, NaturalUnit::Match);
            meta.match_count = rendered.match_count;
            meta.file_count = None;
            ReadSegment {
                body: rendered.content,
                affordance,
                images,
                history: None,
                meta,
            }
        }
        NaturalUnit::Record => {
            // Body is already item-windowed by the producer; report item counts
            // and an item offset, and let the view render the header/footer
            // without re-windowing.
            let mut meta = SegmentMeta::new(target, SegmentKind::Resource, NaturalUnit::Record);
            meta.total_units = rendered.total_units;
            meta.shown_units = rendered
                .shown_units
                .unwrap_or_else(|| rendered.content.lines().count());
            meta.unit_noun = rendered.unit_noun.map(str::to_string);
            meta.offset = rendered.offset.unwrap_or(0).max(0) as usize;
            meta.limit = rendered.limit;
            ReadSegment {
                body: rendered.content,
                affordance,
                images,
                history: None,
                meta,
            }
        }
        NaturalUnit::Turn => {
            let mut meta = SegmentMeta::new(target, SegmentKind::Resource, NaturalUnit::Turn);
            meta.total_units = rendered.total_units;
            meta.shown_units = rendered
                .shown_units
                .unwrap_or_else(|| rendered.content.matches("\n## Turn ").count());
            meta.unit_noun = rendered.unit_noun.map(str::to_string);
            meta.offset = rendered.offset.unwrap_or(0).max(0) as usize;
            meta.limit = rendered.limit;
            ReadSegment {
                body: rendered.content,
                affordance,
                images,
                history: None,
                meta,
            }
        }
        _ => {
            let window =
                view::window_text_lines(&rendered.content, rendered.offset, rendered.limit);
            let mut meta = SegmentMeta::new(target, SegmentKind::Resource, NaturalUnit::Line);
            meta.total_units = Some(window.total);
            meta.shown_units = window.shown;
            meta.offset = window.offset;
            meta.limit = rendered.limit;
            ReadSegment {
                body: window.body,
                affordance,
                images,
                history: None,
                meta,
            }
        }
    }
}

/// A terminal resource segment. The terminal handler returns a structured
/// `TerminalReadResult` whose `output` is a one-line status banner, a blank
/// line, then the rendered buffer. The banner is peeled off and the universal
/// read grammar applies over the buffer alone — exactly like web/resource
/// segments: `grep` greps the rendered text, otherwise the body is line-windowed
/// by `offset`/`limit` (negative offset tails) — then the banner is re-prepended
/// to the surviving slice so every read leads with live status. The
/// terminal-specific params (`new`, `full`, `annotations`) are forwarded to the
/// handler; everything else is stripped here so the handler never sees grammar
/// params it would reject.
async fn produce_terminal_segment(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    read_cursors: &ReadCursorState,
    target: &str,
) -> ReadSegment {
    // Universal grep over the rendered buffer (a single body has no glob pushdown).
    let grep_payload = match split_target_query(target) {
        Ok(split) => match crate::mcp::handlers::search::body_grep_payload(&split.params, false) {
            Ok(payload) => payload,
            Err(error) => return error_segment(target, error),
        },
        Err(_) => None,
    };

    let (clean, offset, limit) = split_line_scope(target, true);
    let clean = strip_query_keys(&clean, GREP_STRIP_KEYS);

    let resource_request = McpCallbackRequest {
        thread_id: None,
        cwd: request.cwd.clone(),
        run_id: request.run_id.clone(),
        tool: "read_resource".to_string(),
        payload: serde_json::json!({ "uri": clean }),
        tool_use_id: request.tool_use_id.clone(),
    };
    let raw = crate::mcp::handlers::resources::handle_read_resource(
        orch,
        &resource_request,
        read_cursors,
    )
    .await;

    // The handler returns a structured result: surface its errors as error
    // segments, otherwise window/grep the `output` text.
    let value = serde_json::from_str::<serde_json::Value>(&raw).ok();
    if value
        .as_ref()
        .and_then(|v| v.get("status").and_then(|s| s.as_str()))
        == Some("error")
    {
        let message = value
            .as_ref()
            .and_then(|v| v.get("output").and_then(|o| o.as_str()))
            .unwrap_or(&raw);
        return error_segment(target, message.to_string());
    }
    let rendered = value
        .as_ref()
        .and_then(|v| v.get("output").and_then(|o| o.as_str()).map(str::to_string))
        .unwrap_or(raw);

    // Peel the one-line status banner (`[terminal <slug>: …]` + blank line) off
    // the front so the universal grammar applies to the buffer alone, then
    // re-prepend it to whatever slice survives. This keeps live status on every
    // read — including `grep` (which would never match it) and tail windows
    // (which start past line 1) — without it counting as a content line for
    // offset/limit or as a grep match. The buffer-byte accounting that `?new=true`
    // relies on lives in the resource handler and is untouched here.
    let (banner, rendered) = peel_terminal_banner(rendered);

    if let Some(payload) = grep_payload {
        let (content, match_count) =
            crate::mcp::handlers::search::grep_materialized_body(&rendered, &payload);
        let content = prepend_terminal_banner(banner.as_deref(), content);
        let mut meta = SegmentMeta::new(target, SegmentKind::Grep, NaturalUnit::Match);
        meta.match_count = Some(match_count);
        meta.file_count = None;
        return ReadSegment::text(content, meta);
    }

    let window = view::window_text_lines(&rendered, offset, limit);
    let body = prepend_terminal_banner(banner.as_deref(), window.body);
    let mut meta = SegmentMeta::new(target, SegmentKind::Resource, NaturalUnit::Line);
    meta.total_units = Some(window.total);
    meta.offset = window.offset;
    meta.limit = limit;
    ReadSegment::text(body, meta)
}

/// Split the leading status banner (`[terminal …]` followed by a blank line) off
/// a rendered terminal body. Returns `(Some(banner), buffer)` when the prefix is
/// present, else `(None, body)` unchanged. The banner is a single line, so the
/// first `\n\n` is always the separator the resource handler inserted.
fn peel_terminal_banner(rendered: String) -> (Option<String>, String) {
    if rendered.starts_with("[terminal ") {
        if let Some((banner, rest)) = rendered.split_once("\n\n") {
            return (Some(banner.to_string()), rest.to_string());
        }
    }
    (None, rendered)
}

/// Re-attach a peeled status banner above a windowed/grepped buffer slice, so
/// every terminal read leads with live status regardless of filter. Presentation
/// only: the banner is never grep-matched and never counted in offset/limit.
fn prepend_terminal_banner(banner: Option<&str>, body: String) -> String {
    match banner {
        Some(banner) if body.is_empty() => banner.to_string(),
        Some(banner) => format!("{banner}\n\n{body}"),
        None => body,
    }
}

/// Strip the client-side line-window params (`offset`, and `limit` when
/// `include_limit`) out of a target's query, returning the target rebuilt
/// without them plus the parsed values. The target is left byte-for-byte when
/// there is nothing to strip, so web URLs and projection queries are preserved.
fn split_line_scope(target: &str, include_limit: bool) -> (String, Option<i64>, Option<usize>) {
    let split = match split_target_query(target) {
        Ok(split) => split,
        Err(_) => return (target.to_string(), None, None),
    };
    let offset = split
        .params
        .iter()
        .rev()
        .find(|param| param.key == "offset")
        .and_then(|param| param.value.parse::<i64>().ok());
    let limit = if include_limit {
        split
            .params
            .iter()
            .rev()
            .find(|param| param.key == "limit")
            .and_then(|param| param.value.parse::<usize>().ok())
    } else {
        None
    };
    if offset.is_none() && limit.is_none() {
        return (target.to_string(), None, None);
    }
    let remaining: Vec<QueryParam> = split
        .params
        .into_iter()
        .filter(|param| param.key != "offset" && (!include_limit || param.key != "limit"))
        .collect();
    let clean = if remaining.is_empty() {
        split.identity
    } else {
        format!("{}?{}", split.identity, encode_query_params(&remaining))
    };
    (clean, offset, limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::read::{Affordance, NaturalUnit, ReadBatchEnvelope, SegmentKind};

    fn file_segment(uri: &str, lines: usize) -> ReadSegment {
        let body: String = (1..=lines)
            .map(|i| format!("{:>6}\tline {i}", i))
            .collect::<Vec<_>>()
            .join("\n");
        let mut meta = SegmentMeta::new(uri, SegmentKind::File, NaturalUnit::Line);
        meta.total_units = Some(lines);
        ReadSegment::text(body, meta)
    }

    #[test]
    fn assemble_orders_targets_under_headers() {
        let envelope = view::assemble(vec![
            file_segment("file:a.rs", 2),
            file_segment("file:b.rs", 2),
        ]);
        let a = envelope.text.find("=== file:a.rs").unwrap();
        let b = envelope.text.find("=== file:b.rs").unwrap();
        assert!(a < b);
        assert_eq!(envelope.segments.len(), 2);
    }

    #[test]
    fn assemble_keeps_every_target_under_one_budget() {
        // One large target plus several small ones: all represented, total under
        // the budget, small targets rendered whole.
        let mut segments = vec![file_segment("file:big.rs", 4_000)];
        for i in 0..5 {
            segments.push(file_segment(&format!("file:small{i}.rs"), 3));
        }
        let envelope = view::assemble(segments);
        assert!(envelope.text.len() <= view::READ_BATCH_CHAR_BUDGET);
        for i in 0..5 {
            assert!(envelope.text.contains(&format!("=== file:small{i}.rs")));
        }
        assert!(envelope.text.contains("=== file:big.rs"));
        assert_eq!(envelope.segments.len(), 6);
    }

    #[test]
    fn assemble_dedupes_affordances_after_content() {
        let affordance = Affordance {
            kind: SegmentKind::Resource,
            block: "--- actions ---".to_string(),
        };
        let mut a = file_segment("cairn://p/X/1", 2);
        a.meta.kind = SegmentKind::Resource;
        a.affordance = Some(affordance.clone());
        let mut b = file_segment("cairn://p/X/2", 2);
        b.meta.kind = SegmentKind::Resource;
        b.affordance = Some(affordance.clone());
        let envelope = view::assemble(vec![a, b]);
        // The identical affordance appears exactly once, at the very end.
        assert_eq!(envelope.text.matches("--- actions ---").count(), 1);
        assert!(envelope.text.trim_end().ends_with("--- actions ---"));
    }

    #[test]
    fn assemble_surfaces_error_segment_in_place() {
        let envelope = view::assemble(vec![
            file_segment("file:a.rs", 2),
            error_segment("file:bad.rs", "Failed to read file: nope"),
        ]);
        assert!(envelope.text.contains("=== file:bad.rs ==="));
        assert!(envelope.text.contains("Failed to read file: nope"));
    }

    #[test]
    fn split_line_scope_strips_offset_keeps_other_params() {
        let (clean, offset, limit) =
            split_line_scope("cairn://p/X/1/messages?status=active&offset=5", false);
        assert_eq!(clean, "cairn://p/X/1/messages?status=active");
        assert_eq!(offset, Some(5));
        assert_eq!(limit, None);
    }

    #[test]
    fn split_line_scope_leaves_url_unchanged_without_scope() {
        let (clean, offset, limit) = split_line_scope("https://example.com/p?a=b", true);
        assert_eq!(clean, "https://example.com/p?a=b");
        assert_eq!(offset, None);
        assert_eq!(limit, None);
    }

    #[tokio::test]
    async fn read_batch_composes_real_file_targets() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
        use std::sync::Arc;

        let worktree = tempfile::tempdir().unwrap();
        let big: String = (1..=40)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(worktree.path().join("big.rs"), &big).unwrap();
        std::fs::write(worktree.path().join("small.rs"), "hello\nworld").unwrap();

        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();

        let request = McpCallbackRequest {
            thread_id: None,
            cwd: worktree.path().display().to_string(),
            run_id: None,
            tool: "read_batch".to_string(),
            payload: serde_json::json!({
                "paths": ["file:big.rs?offset=2&limit=3", "file:small.rs"]
            }),
            tool_use_id: None,
        };
        let cursors = Mutex::new(HashMap::new());
        let result = handle_read_batch(&orch, &request, &cursors).await;
        let envelope: ReadBatchEnvelope = serde_json::from_str(&result).unwrap();

        // Both targets are represented, in order.
        let big_at = envelope
            .text
            .find("=== file:big.rs?offset=2&limit=3")
            .unwrap();
        let small_at = envelope.text.find("=== file:small.rs").unwrap();
        assert!(big_at < small_at);
        // The windowed file carries the enriched line suffix and a valid continue
        // footer; the small file renders whole.
        assert!(envelope
            .text
            .contains("=== file:big.rs?offset=2&limit=3 [lines 3\u{2013}5 of 40] ==="));
        assert!(envelope.text.contains("continue: file:big.rs?offset=5"));
        assert!(envelope
            .text
            .contains("=== file:small.rs [lines 1\u{2013}2 of 2] ==="));
        assert!(envelope.text.contains("hello"));
        // Metadata rides the envelope losslessly.
        assert_eq!(envelope.segments.len(), 2);
        assert_eq!(envelope.segments[0].total_units, Some(40));
        assert_eq!(envelope.segments[0].offset, 2);
        assert_eq!(envelope.segments[0].shown_units, 3);
    }

    /// Run a `read_batch` with an explicit worktree `cwd`, returning the
    /// assembled envelope text. Mirrors `read_batch_text` but lets a test point
    /// the worktree at a fixture directory so bare-path resolution can be
    /// exercised against real files.
    async fn read_batch_text_in(
        orch: &Orchestrator,
        cwd: &std::path::Path,
        paths: serde_json::Value,
    ) -> String {
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: cwd.display().to_string(),
            run_id: None,
            tool: "read_batch".to_string(),
            payload: serde_json::json!({ "paths": paths }),
            tool_use_id: None,
        };
        let cursors = Mutex::new(HashMap::new());
        let result = handle_read_batch(orch, &request, &cursors).await;
        let envelope: ReadBatchEnvelope = serde_json::from_str(&result).unwrap();
        envelope.text
    }

    #[tokio::test]
    async fn read_batch_bare_worktree_path_resolves_as_file() {
        // CAIRN-2030: a scheme-less target that exists in the worktree is read as
        // a `file:` target, yielding the same content `file:docs/...` would.
        let orch = seeded_orch().await;
        let worktree = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(worktree.path().join("docs")).unwrap();
        let body: String = (1..=10)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(worktree.path().join("docs/uri-scheme.md"), &body).unwrap();

        let bare = read_batch_text_in(
            &orch,
            worktree.path(),
            serde_json::json!(["docs/uri-scheme.md"]),
        )
        .await;
        let prefixed = read_batch_text_in(
            &orch,
            worktree.path(),
            serde_json::json!(["file:docs/uri-scheme.md"]),
        )
        .await;
        // The bare target is routed as `file:` (header and content match the
        // explicit form).
        assert!(bare.contains("=== file:docs/uri-scheme.md"), "{bare}");
        assert!(bare.contains("line 1"), "{bare}");
        assert!(bare.contains("line 10"), "{bare}");
        assert_eq!(bare, prefixed);

        // A leading `./` resolves the same way.
        let dotted = read_batch_text_in(
            &orch,
            worktree.path(),
            serde_json::json!(["./docs/uri-scheme.md"]),
        )
        .await;
        assert!(dotted.contains("line 1"), "{dotted}");
    }

    #[tokio::test]
    async fn read_batch_bare_worktree_path_honors_query() {
        // The exact repro: `docs/uri-scheme.md?limit=3`. The query is split off
        // before the existence check and still applied to the file read.
        let orch = seeded_orch().await;
        let worktree = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(worktree.path().join("docs")).unwrap();
        let body: String = (1..=10)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(worktree.path().join("docs/uri-scheme.md"), &body).unwrap();

        let text = read_batch_text_in(
            &orch,
            worktree.path(),
            serde_json::json!(["docs/uri-scheme.md?limit=3"]),
        )
        .await;
        assert!(
            text.contains("=== file:docs/uri-scheme.md?limit=3 [lines 1\u{2013}3 of 10] ==="),
            "{text}"
        );
        assert!(text.contains("line 3"), "{text}");
        assert!(!text.contains("line 4"), "{text}");
    }

    #[tokio::test]
    async fn read_batch_bare_worktree_directory_resolves() {
        // A bare path naming a directory resolves like a `file:` directory read.
        let orch = seeded_orch().await;
        let worktree = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(worktree.path().join("docs")).unwrap();
        std::fs::write(worktree.path().join("docs/uri-scheme.md"), "x").unwrap();

        let text = read_batch_text_in(&orch, worktree.path(), serde_json::json!(["docs"])).await;
        assert!(text.contains("=== file:docs"), "{text}");
        assert!(text.contains("uri-scheme.md"), "{text}");
    }

    #[tokio::test]
    async fn read_batch_bare_nonexistent_path_still_errors() {
        // A bare path that does not exist (a typo) is not silently swallowed: it
        // still produces the clear invalid-target error.
        let orch = seeded_orch().await;
        let worktree = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(worktree.path().join("docs")).unwrap();

        let text = read_batch_text_in(
            &orch,
            worktree.path(),
            serde_json::json!(["docs/does-not-exist.md"]),
        )
        .await;
        assert!(text.contains("Invalid target: expected cairn://"), "{text}");
    }

    #[tokio::test]
    async fn read_batch_bare_absolute_path_outside_worktree_still_errors() {
        // An existing absolute path that resolves OUTSIDE the worktree is not the
        // unambiguous in-worktree intent the leniency covers, so it still errors.
        let orch = seeded_orch().await;
        let worktree = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.txt");
        std::fs::write(&outside_file, "nope").unwrap();

        let text = read_batch_text_in(
            &orch,
            worktree.path(),
            serde_json::json!([outside_file.display().to_string()]),
        )
        .await;
        assert!(text.contains("Invalid target: expected cairn://"), "{text}");
    }

    async fn seeded_orch() -> Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
        use std::sync::Arc;

        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    async fn exec(orch: &Orchestrator, sql: &'static str) {
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(sql, ()).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    async fn read_batch_text(orch: &Orchestrator, paths: serde_json::Value) -> String {
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: tempfile::tempdir().unwrap().keep().display().to_string(),
            run_id: None,
            tool: "read_batch".to_string(),
            payload: serde_json::json!({ "paths": paths }),
            tool_use_id: None,
        };
        let cursors = Mutex::new(HashMap::new());
        let result = handle_read_batch(orch, &request, &cursors).await;
        let envelope: ReadBatchEnvelope = serde_json::from_str(&result).unwrap();
        envelope.text
    }

    async fn seed_project(orch: &Orchestrator) {
        exec(
            orch,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-rb', 'default', 'RB', 'RB', '/tmp/repo', 1, 1)",
        )
        .await;
    }

    async fn seed_home_run(orch: &Orchestrator) {
        seed_project(orch).await;
        exec(
            orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-rb', 'recipe', 'issue-rb-1', 'proj-rb', 'running', 1, 1)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-rb', 'exec-rb', 'issue-rb-1', 'proj-rb', 'Builder', 'running', 1, 1, 'builder', '/tmp/repo-builder')",
        )
        .await;
        exec(
            orch,
            "INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at, start_mode)
             VALUES ('run-rb', 'proj-rb', 'issue-rb-1', 'job-rb', 'live', 1, 1, 'resume')",
        )
        .await;
    }

    #[tokio::test]
    async fn read_batch_bare_home_resolves_to_node_summary() {
        let orch = seeded_orch().await;
        seed_home_run(&orch).await;

        let envelope =
            read_batch_envelope_for_run(&orch, serde_json::json!(["cairn:~"]), Some("run-rb"))
                .await;

        assert!(envelope.text.contains("=== cairn:~"), "{}", envelope.text);
        assert!(envelope.text.contains("# builder [◐]"), "{}", envelope.text);
        assert!(envelope.text.contains("## Resources"), "{}", envelope.text);
        assert!(
            envelope.text.contains("cairn://p/RB/1/1/builder/chat"),
            "{}",
            envelope.text
        );
        assert!(
            !envelope.text.contains("Invalid target"),
            "{}",
            envelope.text
        );
        assert_eq!(envelope.segments[0].kind, SegmentKind::Resource);
    }

    #[test]
    fn cairn_home_targets_are_classified_as_resources() {
        assert!(is_cairn_resource_target("cairn://p/X/1"));
        assert!(is_cairn_resource_target("cairn:~"));
        assert!(is_cairn_resource_target("cairn:~/todos"));
        assert!(!is_cairn_resource_target("cairn:other"));
        assert!(!is_cairn_resource_target("file:src/lib.rs"));
    }

    #[tokio::test]
    async fn read_batch_dedupes_issue_affordance_across_targets() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-2', 'proj-rb', 2, 'Second', 'active', 1, 2)",
        )
        .await;

        let text = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/1", "cairn://p/RB/2"]),
        )
        .await;

        // Each issue keeps its own header...
        assert!(text.contains("=== cairn://p/RB/1"));
        assert!(text.contains("=== cairn://p/RB/2"));
        // ...but the templated issue affordance is byte-identical across the two
        // and dedupes to a single titled block at the end.
        assert_eq!(text.matches("## Issue details").count(), 1);
        assert_eq!(text.matches("### actions").count(), 1);
        assert!(text.contains("- [append comment](cairn://p/{project}/{number}):"));
    }

    #[tokio::test]
    async fn read_batch_titles_distinct_kind_affordances_separately() {
        // A batch spanning two distinct resource kinds renders two affordance
        // blocks, each TITLED with its resource kind, so concatenated blocks are
        // never anonymous. The `(kind, block)` dedup only collapses same-kind
        // instances, so the two titles each appear exactly once.
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;

        let text = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/1", "cairn://p/RB/1/messages"]),
        )
        .await;

        // Two distinct, kind-titled affordance blocks, each rendered once.
        assert_eq!(text.matches("## Issue details").count(), 1, "{text}");
        assert_eq!(text.matches("## Issue messages").count(), 1, "{text}");
    }

    #[tokio::test]
    async fn read_batch_renders_artifact_as_multiline_markdown() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-rb', 'recipe', 'issue-rb-1', 'proj-rb', 'running', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-rb', 'exec-rb', 'issue-rb-1', 'proj-rb', 'Planner', 'complete', 1, 1, 'planner', '/tmp/repo-planner')",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO artifacts(id, job_id, artifact_type, schema_version, data, version, output_name, created_at, updated_at)
             VALUES ('art-rb', 'job-rb', 'result', 1, '{\"content\":\"line1\\nline2\\nline3\"}', 1, 'result', 1, 1)",
        )
        .await;

        let text = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/1/1/planner/result"]),
        )
        .await;

        // The stored single-field JSON renders to real lines, not a one-line blob.
        assert!(text.contains("line1\nline2\nline3"));
        assert!(!text.contains("{\"content\""));
        assert!(text.contains("[lines 1\u{2013}3 of 3]"));
    }

    #[tokio::test]
    async fn read_batch_issues_paging_header_and_continue() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        for (id, number, updated) in [
            ("issue-rb-1", 1, 1),
            ("issue-rb-2", 2, 2),
            ("issue-rb-3", 3, 3),
        ] {
            let sql = format!(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at) VALUES ('{id}', 'proj-rb', {number}, 'Issue', 'active', 1, {updated})"
            );
            orch.db
                .local
                .write(move |conn| {
                    let sql = sql.clone();
                    Box::pin(async move {
                        conn.execute(&sql, ()).await?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
        }

        let text = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/issues?limit=1&offset=1"]),
        )
        .await;

        // Header reports the true total and the page advances by one issue.
        assert!(text.contains("[1 of 3 issues]"));
        assert!(text.contains("offset=2"));
    }

    #[tokio::test]
    async fn read_batch_chat_limit_is_windowed_not_rejected() {
        // Regression (CAIRN-1581): `.../chat?limit=N` used to bounce as an
        // unsupported query param; central view-param consumption windows it.
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-rb', 'recipe', 'issue-rb-1', 'proj-rb', 'running', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-rb', 'exec-rb', 'issue-rb-1', 'proj-rb', 'Planner', 'complete', 1, 1, 'planner', '/tmp/repo-planner')",
        )
        .await;

        let text = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/1/1/planner/chat?limit=2"]),
        )
        .await;

        assert!(!text.to_lowercase().contains("unsupported"));
        assert!(!text.contains("not supported"));
    }

    #[test]
    fn web_targets_are_classified() {
        assert!(is_web_target("https://example.com"));
        assert!(is_web_target("http://example.com/x"));
        assert!(is_web_target("file:docs/design.PDF?offset=1"));
        assert!(!is_web_target("file:src/lib.rs"));
        assert!(!is_web_target("cairn://p/X/1"));
    }

    #[test]
    fn browser_resource_targets_are_classified_for_dedup_exemption() {
        assert!(is_browser_resource_target("cairn://p/X/browser"));
        assert!(is_browser_resource_target(
            "cairn://p/X/browser/main?format=text"
        ));
        assert!(is_browser_resource_target(
            "cairn://p/X/1/1/builder/browser"
        ));
        assert!(is_browser_resource_target(
            "cairn://p/X/1/1/builder/task/explore/browser/preview?screenshot"
        ));
        assert!(!is_browser_resource_target("cairn://p/X/1"));
        assert!(!is_browser_resource_target("file:src/lib.rs"));
    }

    async fn read_batch_envelope(
        orch: &Orchestrator,
        paths: serde_json::Value,
    ) -> ReadBatchEnvelope {
        read_batch_envelope_for_run(orch, paths, None).await
    }

    async fn read_batch_envelope_for_run(
        orch: &Orchestrator,
        paths: serde_json::Value,
        run_id: Option<&str>,
    ) -> ReadBatchEnvelope {
        let request = McpCallbackRequest {
            thread_id: None,
            cwd: tempfile::tempdir().unwrap().keep().display().to_string(),
            run_id: run_id.map(str::to_string),
            tool: "read_batch".to_string(),
            payload: serde_json::json!({ "paths": paths }),
            tool_use_id: None,
        };
        let cursors = Mutex::new(HashMap::new());
        let result = handle_read_batch(orch, &request, &cursors).await;
        serde_json::from_str(&result).unwrap()
    }

    fn register_turn(orch: &Orchestrator, run_id: &str, turn_id: &str) {
        use crate::agent_process::process::RunHandle;

        let mut processes = orch.process_state.processes.lock().unwrap();
        processes.register(
            run_id.to_string(),
            RunHandle::test_handle(Some("sess"), None),
        );
        drop(processes);
        assert!(orch.process_state.begin_turn(run_id, turn_id));
    }

    #[tokio::test]
    async fn read_batch_dedupes_repeated_non_browser_reads() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;
        register_turn(&orch, "run-rb", "turn-rb");

        let first = read_batch_envelope_for_run(
            &orch,
            serde_json::json!(["cairn://p/RB/1"]),
            Some("run-rb"),
        )
        .await;
        assert!(!first.text.contains("[duplicate call]"), "{}", first.text);

        let second = read_batch_envelope_for_run(
            &orch,
            serde_json::json!(["cairn://p/RB/1"]),
            Some("run-rb"),
        )
        .await;
        assert!(second.text.contains("[duplicate call]"), "{}", second.text);
    }

    #[tokio::test]
    async fn read_batch_collapses_repeated_affordance_to_pointer() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-af-1', 'proj-rb', 1, 'First', 'active', 1, 1),
                    ('issue-af-2', 'proj-rb', 2, 'Second', 'active', 1, 1)",
        )
        .await;
        register_turn(&orch, "run-af", "turn-af");

        // Batch 1: first read of the Issue kind shows the full affordance block.
        let first = read_batch_envelope_for_run(
            &orch,
            serde_json::json!(["cairn://p/RB/1"]),
            Some("run-af"),
        )
        .await;
        assert!(first.text.contains("## Issue details"), "{}", first.text);
        assert!(first.text.contains("### actions"), "{}", first.text);
        assert!(
            !first.text.contains("cairn://help?kind=issue"),
            "{}",
            first.text
        );

        // Batch 2: a different Issue instance is the same affordance block, so it
        // collapses to the one-line pointer, not the full block. A distinct URI
        // means flail dedup does not stub the read itself.
        let second = read_batch_envelope_for_run(
            &orch,
            serde_json::json!(["cairn://p/RB/2"]),
            Some("run-af"),
        )
        .await;
        assert!(
            second.text.contains("cairn://help?kind=issue"),
            "{}",
            second.text
        );
        assert!(!second.text.contains("### actions"), "{}", second.text);
    }

    #[tokio::test]
    async fn read_batch_tracks_distinct_affordances_independently() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-af2-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;
        register_turn(&orch, "run-af2", "turn-af2");

        // One batch, two distinct kinds is two distinct affordance blocks. Both
        // are first-seen this session, so both full blocks survive. Hash keying
        // is what keeps them apart: a raw `ResourceKind` key would be fine here
        // but would wrongly collapse the two schema-distinct artifact blocks the
        // hash key protects.
        let batch = read_batch_envelope_for_run(
            &orch,
            serde_json::json!(["cairn://p/RB/1", "cairn://p/RB/1/messages"]),
            Some("run-af2"),
        )
        .await;
        assert!(batch.text.contains("## Issue details"), "{}", batch.text);
        assert!(batch.text.contains("## Issue messages"), "{}", batch.text);
    }

    #[tokio::test]
    async fn read_batch_does_not_dedupe_repeated_browser_reads() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        register_turn(&orch, "run-rb", "turn-rb");

        let first = read_batch_envelope_for_run(
            &orch,
            serde_json::json!(["cairn://p/RB/browser/main"]),
            Some("run-rb"),
        )
        .await;
        assert!(first.text.contains("# Browser main"), "{}", first.text);
        assert!(!first.text.contains("[duplicate call]"), "{}", first.text);

        let second = read_batch_envelope_for_run(
            &orch,
            serde_json::json!(["cairn://p/RB/browser/main"]),
            Some("run-rb"),
        )
        .await;
        assert!(second.text.contains("# Browser main"), "{}", second.text);
        assert!(!second.text.contains("[duplicate call]"), "{}", second.text);
    }

    #[tokio::test]
    async fn grep_over_issue_overview_yields_match_segment() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'Alphaneedle', 'active', 1, 1)",
        )
        .await;

        let envelope = read_batch_envelope(
            &orch,
            serde_json::json!(["cairn://p/RB/1?grep=Alphaneedle"]),
        )
        .await;

        let seg = &envelope.segments[0];
        assert_eq!(seg.kind, SegmentKind::Grep);
        assert_eq!(seg.natural_unit, NaturalUnit::Match);
        assert!(seg.match_count.unwrap() >= 1);
        // A single materialized body has no file dimension.
        assert_eq!(seg.file_count, None);
        // The enriched header carries the path-less `[N matches]` suffix.
        assert!(envelope.text.contains(" matches] ==="), "{}", envelope.text);
        // Line-number-prefixed, path-less output (`N:...`), never a path prefix.
        assert!(
            !envelope.text.contains("cairn://p/RB/1:"),
            "{}",
            envelope.text
        );
    }

    #[tokio::test]
    async fn grep_over_issues_greps_full_filtered_set() {
        // Under grep, `/issues` must render the entire filtered set rather than
        // the default 20-issue page, so matches in issues that would fall on a
        // later page are still found. The five `RareNeedle` issues are the
        // oldest (page 2+), so a paged render would miss them entirely.
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        for number in 1..=25 {
            let title = if number <= 5 { "RareNeedle" } else { "Common" };
            let sql = format!(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at) VALUES ('issue-rb-{number}', 'proj-rb', {number}, '{title}', 'active', 1, {number})"
            );
            orch.db
                .local
                .write(move |conn| {
                    let sql = sql.clone();
                    Box::pin(async move {
                        conn.execute(&sql, ()).await?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
        }

        let envelope = read_batch_envelope(
            &orch,
            serde_json::json!(["cairn://p/RB/issues?status=active&grep=RareNeedle"]),
        )
        .await;

        let seg = &envelope.segments[0];
        assert_eq!(seg.kind, SegmentKind::Grep);
        assert_eq!(seg.match_count, Some(5), "{}", envelope.text);
    }

    #[tokio::test]
    async fn grep_over_artifact_is_line_number_prefixed() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-rb', 'recipe', 'issue-rb-1', 'proj-rb', 'running', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-rb', 'exec-rb', 'issue-rb-1', 'proj-rb', 'Planner', 'complete', 1, 1, 'planner', '/tmp/repo-planner')",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO artifacts(id, job_id, artifact_type, schema_version, data, version, output_name, created_at, updated_at)
             VALUES ('art-rb', 'job-rb', 'result', 1, '{\"content\":\"line1\\nline2\\nline3\"}', 1, 'result', 1, 1)",
        )
        .await;

        let envelope = read_batch_envelope(
            &orch,
            serde_json::json!(["cairn://p/RB/1/1/planner/result?grep=line2"]),
        )
        .await;

        let seg = &envelope.segments[0];
        assert_eq!(seg.kind, SegmentKind::Grep);
        assert_eq!(seg.match_count, Some(1), "{}", envelope.text);
        // Path-less, line-number-prefixed match line.
        assert!(envelope.text.contains("2:line2"), "{}", envelope.text);
    }

    #[tokio::test]
    async fn grep_over_changed_composes_with_glob_pushdown() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-rb', 'recipe', 'issue-rb-1', 'proj-rb', 'running', 1, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-rb', 'exec-rb', 'issue-rb-1', 'proj-rb', 'Planner', 'complete', 1, 1, 'planner', '/tmp/repo-planner')",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO file_changes(id, job_id, file_path, status, additions, deletions, created_at)
             VALUES ('fc-1', 'job-rb', 'src/keep.rs', 'modified', 1, 0, 1)",
        )
        .await;
        exec(
            &orch,
            "INSERT INTO file_changes(id, job_id, file_path, status, additions, deletions, created_at)
             VALUES ('fc-2', 'job-rb', 'notes.md', 'modified', 1, 0, 1)",
        )
        .await;

        let envelope = read_batch_envelope(
            &orch,
            serde_json::json!(["cairn://p/RB/1/changed?glob=**/*.rs&grep=keep"]),
        )
        .await;

        let seg = &envelope.segments[0];
        assert_eq!(seg.kind, SegmentKind::Grep);
        // glob pushdown drops notes.md before grep; only src/keep.rs remains.
        assert_eq!(seg.match_count, Some(1), "{}", envelope.text);
        assert!(envelope.text.contains("keep.rs"), "{}", envelope.text);
        assert!(!envelope.text.contains("notes.md"), "{}", envelope.text);
    }

    #[tokio::test]
    async fn grep_with_offset_is_rejected_on_a_resource() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        exec(
            &orch,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-rb-1', 'proj-rb', 1, 'First', 'active', 1, 1)",
        )
        .await;

        let text = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/1?grep=First&offset=2"]),
        )
        .await;

        assert!(text.contains("does not combine with 'grep'"), "{text}");
    }

    async fn seed_project_terminal(orch: &Orchestrator) {
        seed_project(orch).await;
        exec(
            orch,
            "INSERT INTO job_terminals
               (id, job_id, project_id, run_id, session_id, command, status, created_at, slug)
             VALUES ('term-rb', NULL, 'proj-rb', NULL, 'sess-rb', 'sleep 1', 'running', 1, 'ci')",
        )
        .await;
    }

    #[tokio::test]
    async fn terminal_target_accepts_universal_grammar_through_batch() {
        let orch = seeded_orch().await;
        seed_project_terminal(&orch).await;

        for path in [
            "cairn://p/RB/terminal/ci",
            "cairn://p/RB/terminal/ci?limit=50",
            "cairn://p/RB/terminal/ci?offset=-20",
        ] {
            let text = read_batch_text(&orch, serde_json::json!([path])).await;
            assert!(!text.contains("Invalid terminal URI"), "{path}: {text}");
            assert!(
                !text.contains("Unsupported query parameter"),
                "{path}: {text}"
            );
            assert!(text.contains("[terminal ci:"), "{path}: {text}");
        }
    }

    // An exited terminal with a known multi-line retained buffer, so tail windows
    // and grep actually exercise the banner-peel boundary (a one-line running
    // terminal is too small for a tail to ever drop line 1).
    async fn seed_project_terminal_with_buffer(orch: &Orchestrator) {
        seed_project(orch).await;
        exec(
            orch,
            "INSERT INTO job_terminals
               (id, job_id, project_id, run_id, session_id, command, status, exit_code, created_at, exited_at, slug, output_tail)
             VALUES ('term-rb-buf', NULL, 'proj-rb', NULL, 'sess-rb-buf', 'print logs', 'exited', 0, 1, 9, 'logs',
               'alpha-1
beta-2
gamma-3
delta-4
epsilon-5
zeta-6
eta-7
theta-8
iota-9
kappa-10
lambda-11
mu-12
nu-13
xi-14
omicron-15
pi-16
rho-17
sigma-18
tau-19
upsilon-20
phi-21
chi-22
psi-23
omega-24')",
        )
        .await;
    }

    #[tokio::test]
    async fn terminal_grep_routes_to_grep_segment() {
        let orch = seeded_orch().await;
        seed_project_terminal_with_buffer(&orch).await;
        let envelope = read_batch_envelope(
            &orch,
            serde_json::json!(["cairn://p/RB/terminal/logs?grep=gamma"]),
        )
        .await;
        let seg = &envelope.segments[0];
        assert_eq!(seg.kind, SegmentKind::Grep, "{}", envelope.text);
        // Grep matches the buffer line, not the banner.
        assert!(seg.match_count.unwrap_or(0) >= 1, "{}", envelope.text);
        assert!(envelope.text.contains("gamma-3"), "{}", envelope.text);
    }

    // The status banner survives grep and tail slices that would otherwise drop
    // line 1, and the banner itself is never counted as a grep match.
    #[tokio::test]
    async fn terminal_banner_survives_grep_and_tail() {
        let orch = seeded_orch().await;
        seed_project_terminal_with_buffer(&orch).await;

        // Tail window starts well past line 1, yet still leads with the banner
        // and shows the true tail (last line present, first line gone).
        let tail = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/terminal/logs?offset=-5"]),
        )
        .await;
        assert!(
            tail.contains("[terminal logs:"),
            "banner dropped by tail: {tail}"
        );
        assert!(tail.contains("omega-24"), "{tail}");
        assert!(!tail.contains("alpha-1"), "tail leaked the head: {tail}");

        // Grep that matches a buffer line still carries the banner.
        let grep = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/terminal/logs?grep=sigma"]),
        )
        .await;
        assert!(
            grep.contains("[terminal logs:"),
            "banner dropped by grep: {grep}"
        );
        assert!(grep.contains("sigma-18"), "{grep}");

        // The banner is not grep-matchable: a word that lives only in the banner
        // (`exited`) yields zero buffer matches, yet the banner is still shown.
        let envelope = read_batch_envelope(
            &orch,
            serde_json::json!(["cairn://p/RB/terminal/logs?grep=exited"]),
        )
        .await;
        assert_eq!(
            envelope.segments[0].match_count,
            Some(0),
            "{}",
            envelope.text
        );
        assert!(
            envelope.text.contains("[terminal logs:"),
            "banner dropped on zero-match grep: {}",
            envelope.text
        );
    }

    #[tokio::test]
    async fn terminal_missing_slug_errors_with_existing_slugs() {
        let orch = seeded_orch().await;
        seed_project_terminal(&orch).await;
        let text = read_batch_text(&orch, serde_json::json!(["cairn://p/RB/terminal/nope"])).await;
        assert!(text.contains("nope"), "{text}");
        assert!(text.contains("ci"), "{text}");
    }

    #[tokio::test]
    async fn task_terminal_read_via_batch_surfaces_terminal_shape() {
        let orch = seeded_orch().await;
        seed_project(&orch).await;
        let text = read_batch_text(
            &orch,
            serde_json::json!(["cairn://p/RB/1/1/builder/task/Explore/terminal/ci"]),
        )
        .await;
        // Terminal reads are served by read_resource; the batch path falls back to
        // the task-terminal affordance (shape + start/send/stop actions) rather than
        // routing. Task terminals are first-class, so there is no top-level-node gate.
        assert!(text.contains("## Task terminal"), "{text}");
        assert!(text.contains("start terminal"), "{text}");
    }
}
