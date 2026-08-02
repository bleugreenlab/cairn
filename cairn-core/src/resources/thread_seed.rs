//! Composing the seed a thread session resumes from (CAIRN-3388).
//!
//! An ordinary issue reseeds from its whole transcript at full fidelity, which
//! is right for work with an objective and an ending. A thread has neither, so
//! the same digest gets more expensive every week while carrying less of what
//! matters. This composer replaces it with three parts, in this order:
//!
//! 1. **The authored arc** — the thread's own record of intent: rulings with
//!    provenance, open questions, current intent. Read from the `arc` artifact
//!    and copied, never summarized and never generated. An unprovenanced ruling
//!    gets relitigated, and a generated summary of one's own decisions is
//!    strictly worse than the version the thread wrote.
//! 2. **A generated census of the children** — where every child issue stands
//!    *right now*, read from live state at composition time. Same split as the
//!    arc, applied to the other half of a thread's memory: the arc is inherited
//!    verbatim, so any census authored into it is stale by construction, and a
//!    resumed thread that cannot trust it spends its first turn re-reading every
//!    child to rebuild what Cairn already knows.
//! 3. **A generated table of contents** — mechanical. A chapter is usually a
//!    child issue: it has a beginning, an end, a subject, and a URI. Everything
//!    else is bounded at user-turn boundaries. Every line carries a one-line
//!    overview *and* an address, because a bare address invites a defensive
//!    re-fetch, which costs more than never having dropped the range.
//! 4. **The recency window** — the last [`RECENCY_TURNS`] turns verbatim, plus
//!    any older turn that still touches an unresolved child.
//!
//! Nothing here decides *when* to compact; that is the trigger's job in
//! `execution::jobs::lifecycle`. Composition is pure enough to run on every
//! continuation of a thread, because its result is also what prices the
//! decision: `source_bytes` is what the compacted turns weigh verbatim, and
//! `candidate_bytes` is what they weigh as chapters.

use cairn_db::turso::params;

use crate::storage::{LocalDb, RowExt};
use crate::threads::compaction::{self, ChildMark, EntrySource, TocEntry};

use super::common::{connect_for_read, job_status_icon};
use super::transcript::{
    format_transcript_digest_with, group_turn_blocks, load_job_events_ordered, DigestMeta,
    DigestOptions, EventRow, TurnBlock,
};

/// How many turns survive verbatim. Compaction is worth doing hard or not at
/// all — halving a prefix takes ~20 turns to pay back its cache write, cutting
/// it to a tenth pays back in ~3 — and children are re-fetchable, which is what
/// makes cutting hard safe.
pub(crate) const RECENCY_TURNS: usize = 10;

/// The artifact a thread authors its arc into.
const ARC_ARTIFACT_NAME: &str = "arc";

const ARC_MISSING_NOTICE: &str = "_No arc has been authored for this thread yet. Write one to `cairn:~/arc` — decisions, the paths you rejected and why, and the questions still open, each with the URI it came from. It is the only part of this context that cannot be regenerated from the transcript, so nothing else in this seed substitutes for it._";

const NO_HISTORY_NOTICE: &str =
    "_Nothing has been compacted yet; the whole session so far is below._";

/// Framing for the children census. It says where the lines came from, because
/// a resumed thread that reads them as prose it once wrote will re-verify them.
const CHILDREN_NOTICE: &str = "_Read from live state as this seed was composed — not authored, not inherited. Trust it over anything the arc says about where a child stands, and re-read a child only when you need detail this line does not carry._";

/// A composed thread seed, and the two measurements that price it.
pub(crate) struct ThreadSeed {
    /// The seed body: arc, table of contents, verbatim window.
    pub content: String,
    /// `P` — what the turns this composition would drop weigh verbatim.
    pub source_bytes: i64,
    /// `p` — what those same turns weigh as table-of-contents lines.
    pub candidate_bytes: i64,
    /// The whole table of contents: entries carried forward plus new ones.
    pub entries: Vec<TocEntry>,
    /// How many entries this composition newly generated.
    pub new_entries: usize,
    pub compacted_through_block: Option<i64>,
    pub recency_start_block: i64,
    /// Marks this composition folded into chapters.
    pub consumed_child_issue_ids: Vec<String>,
    /// The session this seed was composed against. A generation recording it
    /// stays pending until the job leaves that session, which is what makes a
    /// failed rotation retryable.
    pub source_session_id: String,
}

/// Compose the seed for `job`'s next thread session.
///
/// Fails rather than returning a partial seed: the caller preserves the existing
/// session on any error, exactly as the ordinary reseed path does.
pub(crate) async fn compose_thread_seed(
    db: &LocalDb,
    job_id: &str,
    now: i64,
) -> Result<ThreadSeed, String> {
    let conn = connect_for_read(db).await?;
    let events = load_job_events_ordered(
        &conn,
        job_id,
        db.team_id().map(|_| db.content_store().as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;
    if events.is_empty() {
        return Err(
            "Cannot compose a thread seed: this job has no resumable transcript.".to_string(),
        );
    }

    let coordinates = node_coordinates(&conn, job_id).await.ok_or_else(|| {
        "Cannot compose a thread seed: the node has no addressable coordinates.".to_string()
    })?;
    let chat_uri = format!("{}/chat", coordinates.base_uri);

    let source_session_id = current_session_id(&conn, job_id).await.ok_or_else(|| {
        "Cannot compose a thread seed: the job has no current session.".to_string()
    })?;
    let marks = compaction::unconsumed_marks(db, job_id, &source_session_id)
        .await
        .map_err(|error| format!("Failed to load thread compaction marks: {error}"))?;
    let prior_entries = compaction::applied_entries(db, job_id, &source_session_id)
        .await
        .map_err(|error| format!("Failed to load the prior thread table of contents: {error}"))?;
    let children = load_children(&conn, job_id)
        .await
        .map_err(|error| format!("Failed to resolve this thread's children: {error}"))?;
    let open_children: Vec<String> = children
        .iter()
        .filter(|child| child.is_open())
        .map(|child| child.uri.clone())
        .collect();

    let blocks = group_turn_blocks(&events);
    let plan = plan_chapters(&blocks, &marks, &open_children, &prior_entries);

    let new_entries: Vec<TocEntry> = plan
        .chapters
        .iter()
        .map(|chapter| build_entry(chapter, &blocks, &marks, &chat_uri))
        .collect();

    let mut entries = prior_entries;
    for entry in new_entries.iter() {
        if entries
            .iter()
            .any(|kept| kept.dedupe_key() == entry.dedupe_key())
        {
            continue;
        }
        entries.push(entry.clone());
    }
    entries.sort_by_key(|entry| (entry.start_block, entry.end_block));

    let meta = DigestMeta {
        label: &coordinates.label,
        project: &coordinates.project_key,
        number: coordinates.number,
        exec_seq: coordinates.exec_seq,
        status: &coordinates.status,
    };

    let verbatim = render_blocks(&blocks, &plan.verbatim, &chat_uri, &meta);
    let dropped = render_blocks(&blocks, &plan.compacted, &chat_uri, &meta);
    let history = render_history(&entries);
    let candidate = render_history_lines(&new_entries);

    let arc = render_arc(db, job_id).await;
    let carried_older = plan
        .verbatim
        .iter()
        .any(|index| *index < plan.recency_start);

    let mut content = String::new();
    content.push_str("## Arc\n\n");
    content.push_str(arc.trim());
    content.push_str("\n\n");
    content.push_str(&render_children(&children, now));
    content.push_str("## History\n\n");
    content.push_str(&history);
    content.push_str("\n## Recent\n\n");
    if carried_older {
        content.push_str(
            "_Older turns that still reference an unresolved child are kept verbatim below alongside the recent ones._\n\n",
        );
    }
    content.push_str(&verbatim);

    let consumed_child_issue_ids = plan
        .chapters
        .iter()
        .filter_map(|chapter| match chapter.kind {
            ChapterKind::Child(mark) => Some(marks[mark].child_issue_id.clone()),
            ChapterKind::Interstitial => None,
        })
        .collect::<Vec<_>>();

    Ok(ThreadSeed {
        content,
        source_bytes: dropped.len() as i64,
        candidate_bytes: candidate.len() as i64,
        new_entries: new_entries.len(),
        entries,
        compacted_through_block: plan.compacted.last().map(|index| *index as i64),
        recency_start_block: plan.recency_start as i64,
        consumed_child_issue_ids,
        source_session_id,
    })
}

/// The session the job is on right now. Composition is measured against it, and
/// the generation it produces is pending until the job leaves it.
async fn current_session_id(conn: &cairn_db::turso::Connection, job_id: &str) -> Option<String> {
    let mut rows = conn
        .query(
            "SELECT current_session_id FROM jobs WHERE id = ?1 LIMIT 1",
            params![job_id],
        )
        .await
        .ok()?;
    rows.next().await.ok()??.opt_text(0).ok()?
}

// ── Chapter planning ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChapterKind {
    /// Every turn in the range discussed one terminal child.
    Child(usize),
    /// Conversation with no live child in it.
    Interstitial,
}

#[derive(Debug, Clone, Copy)]
struct Chapter {
    kind: ChapterKind,
    start: usize,
    end: usize,
}

struct ChapterPlan {
    chapters: Vec<Chapter>,
    /// Blocks rendered verbatim, in order.
    verbatim: Vec<usize>,
    /// Blocks this composition drops into chapters, in order.
    compacted: Vec<usize>,
    recency_start: usize,
}

/// Decide, per turn, whether it is dropped into a chapter or kept verbatim.
///
/// Classification is per turn rather than per contiguous range on purpose: two
/// children worked in the same stretch of conversation interleave, and treating
/// their combined span as one compactable range would drop turns belonging to
/// whichever child is still open.
fn plan_chapters(
    blocks: &[TurnBlock<'_>],
    marks: &[ChildMark],
    open_children: &[String],
    prior_entries: &[TocEntry],
) -> ChapterPlan {
    let recency_start = blocks.len().saturating_sub(RECENCY_TURNS);
    let already_compacted = |index: usize| {
        prior_entries
            .iter()
            .any(|entry| (entry.start_block..=entry.end_block).contains(&(index as i64)))
    };

    let mut chapters: Vec<Chapter> = Vec::new();
    let mut verbatim: Vec<usize> = Vec::new();
    let mut compacted: Vec<usize> = Vec::new();
    let mut open: Option<Chapter> = None;

    for (index, block) in blocks.iter().enumerate() {
        // A turn already standing behind a chapter from an earlier generation is
        // neither re-compacted nor re-rendered: its entry was carried forward.
        if already_compacted(index) {
            if let Some(chapter) = open.take() {
                chapters.push(chapter);
            }
            continue;
        }
        if index >= recency_start {
            if let Some(chapter) = open.take() {
                chapters.push(chapter);
            }
            verbatim.push(index);
            continue;
        }

        let text = block_text(block);
        if open_children.iter().any(|uri| mentions_uri(&text, uri)) {
            // Never drop a turn that also touches work still in flight.
            if let Some(chapter) = open.take() {
                chapters.push(chapter);
            }
            verbatim.push(index);
            continue;
        }

        let kind = match marks
            .iter()
            .position(|mark| mentions_uri(&text, &mark.child_issue_uri))
        {
            Some(mark) => ChapterKind::Child(mark),
            None => ChapterKind::Interstitial,
        };
        compacted.push(index);

        let extends = match (&open, kind) {
            // Adjacent turns coalesce only when they are about the same child.
            (Some(current), ChapterKind::Child(mark)) => current.kind == ChapterKind::Child(mark),
            // Interstitial conversation is bounded at user-turn boundaries.
            (Some(current), ChapterKind::Interstitial) => {
                current.kind == ChapterKind::Interstitial && !opens_with_user_message(block)
            }
            (None, _) => false,
        };
        match (extends, open.as_mut()) {
            (true, Some(current)) => current.end = index,
            _ => {
                if let Some(chapter) = open.take() {
                    chapters.push(chapter);
                }
                open = Some(Chapter {
                    kind,
                    start: index,
                    end: index,
                });
            }
        }
    }
    if let Some(chapter) = open.take() {
        chapters.push(chapter);
    }

    ChapterPlan {
        chapters,
        verbatim,
        compacted,
        recency_start,
    }
}

fn build_entry(
    chapter: &Chapter,
    blocks: &[TurnBlock<'_>],
    marks: &[ChildMark],
    chat_uri: &str,
) -> TocEntry {
    let (source, overview, content_uri, child_issue_id) = match chapter.kind {
        ChapterKind::Child(mark) => (
            EntrySource::Child,
            one_line(&marks[mark].child_title),
            marks[mark].child_issue_uri.clone(),
            Some(marks[mark].child_issue_id.clone()),
        ),
        ChapterKind::Interstitial => (
            EntrySource::Interstitial,
            interstitial_overview(&blocks[chapter.start]),
            format!(
                "{chat_uri}?offset={}&limit={}",
                chapter.start,
                chapter.end - chapter.start + 1
            ),
            None,
        ),
    };
    TocEntry {
        source,
        overview,
        content_uri,
        child_issue_id,
        start_block: chapter.start as i64,
        end_block: chapter.end as i64,
        start_turn_id: blocks[chapter.start].turn_id.map(str::to_string),
        end_turn_id: blocks[chapter.end].turn_id.map(str::to_string),
    }
}

// ── Rendering ───────────────────────────────────────────────────────────────

fn render_history(entries: &[TocEntry]) -> String {
    if entries.is_empty() {
        return format!("{NO_HISTORY_NOTICE}\n");
    }
    render_history_lines(entries)
}

fn render_history_lines(entries: &[TocEntry]) -> String {
    let mut out = String::new();
    for entry in entries {
        out.push_str(&render_history_line(entry));
    }
    out
}

fn render_history_line(entry: &TocEntry) -> String {
    let range = if entry.start_block == entry.end_block {
        format!("turn {}", entry.start_block + 1)
    } else {
        format!("turns {}–{}", entry.start_block + 1, entry.end_block + 1)
    };
    format!("- {range} · {} → {}\n", entry.overview, entry.content_uri)
}

/// Render a set of turns through the ordinary transcript renderer at reseed
/// fidelity, so kept turns read exactly as they would have without compaction.
///
/// Turns are labelled by their index in the job's whole block sequence, not by
/// their position in the rendered subset, so "turns 1–4" in the table of
/// contents and "Turn 5" in the verbatim section describe one numbering. A
/// subset-relative label would tell a thread its oldest kept turn is turn 1.
fn render_blocks(
    blocks: &[TurnBlock<'_>],
    selected: &[usize],
    chat_uri: &str,
    meta: &DigestMeta<'_>,
) -> String {
    if selected.is_empty() {
        return String::new();
    }
    let events: Vec<EventRow> = selected
        .iter()
        .flat_map(|index| blocks[*index].events.iter())
        .map(|event| (*event).clone())
        .collect();
    let labels: std::collections::HashMap<String, i32> = selected
        .iter()
        .filter_map(|index| {
            blocks[*index]
                .turn_id
                .map(|turn_id| (turn_id.to_string(), *index as i32 + 1))
        })
        .collect();
    format_transcript_digest_with(
        &events,
        chat_uri,
        meta,
        &labels,
        &DigestOptions {
            latest: false,
            turn_offset: None,
            turn_limit: None,
            unabridged: true,
            inline_diffs: true,
        },
    )
}

async fn render_arc(db: &LocalDb, job_id: &str) -> String {
    match crate::artifacts::queries::get_named(db, job_id, ARC_ARTIFACT_NAME).await {
        Ok(Some(artifact)) => render_arc_data(&artifact.data),
        // A thread whose agent has not written an arc still gets a valid seed;
        // it is told, in the seed, that the one irreplaceable section is empty.
        Ok(None) => ARC_MISSING_NOTICE.to_string(),
        Err(error) => {
            log::warn!("thread arc could not be read for job {job_id}: {error}");
            ARC_MISSING_NOTICE.to_string()
        }
    }
}

/// Render the authored arc as stable Markdown, copying every authored string
/// exactly. Anything this does not recognize is passed through rather than
/// dropped: the compactor is not allowed to decide that authored intent was
/// unimportant.
fn render_arc_data(data: &serde_json::Value) -> String {
    let mut out = String::new();

    if let Some(intent) = data
        .get("current_intent")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        out.push_str(intent);
        out.push('\n');
    }

    if let Some(rulings) = data.get("rulings").and_then(|value| value.as_array()) {
        if !rulings.is_empty() {
            out.push_str("\n### Rulings\n\n");
            for ruling in rulings {
                out.push_str(&render_ruling(ruling));
            }
        }
    }

    if let Some(questions) = data
        .get("open_questions")
        .and_then(|value| value.as_array())
    {
        if !questions.is_empty() {
            out.push_str("\n### Open questions\n\n");
            for question in questions {
                let text = question
                    .get("question")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| compact_json(question));
                out.push_str(&format!("- {text}"));
                if let Some(provenance) = provenance_uris(question) {
                    out.push_str(&format!(" (provenance: {provenance})"));
                }
                out.push('\n');
            }
        }
    }

    if out.trim().is_empty() {
        // Not the provenance-first shape. Copy whatever the thread did author.
        return data
            .get("content")
            .or_else(|| data.get("body"))
            .and_then(|value| value.as_str())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| compact_json(data));
    }
    out
}

fn render_ruling(ruling: &serde_json::Value) -> String {
    let text = ruling
        .get("text")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| compact_json(ruling));
    let status = ruling
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("accepted");
    let mut line = format!("- **{status}** — {text}\n");
    if let Some(rationale) = ruling
        .get("rationale")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        line.push_str(&format!("  - why: {rationale}\n"));
    }
    match provenance_uris(ruling) {
        Some(provenance) => line.push_str(&format!("  - provenance: {provenance}\n")),
        // A ruling without provenance gets relitigated, so say so where the
        // reader will see it rather than rendering a confident bare claim.
        None => line.push_str("  - provenance: none recorded\n"),
    }
    line
}

fn provenance_uris(value: &serde_json::Value) -> Option<String> {
    let provenance = value.get("provenance")?;
    let joined = match provenance {
        serde_json::Value::String(uri) => uri.trim().to_string(),
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        _ => String::new(),
    };
    (!joined.is_empty()).then_some(joined)
}

fn compact_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

// ── Turn inspection ─────────────────────────────────────────────────────────

fn block_text(block: &TurnBlock<'_>) -> String {
    let mut text = String::new();
    for event in block.events.iter() {
        text.push_str(&event.data);
        text.push('\n');
    }
    text
}

/// Whether `haystack` names `uri` as a whole issue address.
///
/// Issue URIs are prefixes of each other (`.../339` sits inside `.../3390`), so
/// a plain substring test would attribute one child's turns to another.
fn mentions_uri(haystack: &str, uri: &str) -> bool {
    let mut from = 0;
    while let Some(offset) = haystack[from..].find(uri) {
        let at = from + offset;
        let after = haystack[at + uri.len()..].chars().next();
        if !matches!(after, Some(character) if character.is_ascii_digit()) {
            return true;
        }
        from = at + uri.len();
    }
    false
}

fn opens_with_user_message(block: &TurnBlock<'_>) -> bool {
    user_message(block).is_some()
}

/// The turn's own leading user message, if it has one. Later `user` events in a
/// turn are injections (attention pushes, base-branch updates), not the message
/// that started it, and Cairn's own seed and continuation events are not user
/// messages at all.
fn user_message(block: &TurnBlock<'_>) -> Option<String> {
    for event in block.events.iter() {
        if event.event_type != "user" {
            continue;
        }
        let data = serde_json::from_str::<serde_json::Value>(&event.data).ok()?;
        let content = data.get("content")?.as_str()?;
        if content.contains("<system-reminder>") {
            continue;
        }
        return Some(content.to_string());
    }
    None
}

fn assistant_message(block: &TurnBlock<'_>) -> Option<String> {
    block.events.iter().find_map(|event| {
        if event.event_type != "assistant" {
            return None;
        }
        serde_json::from_str::<serde_json::Value>(&event.data)
            .ok()?
            .get("content")?
            .as_str()
            .map(str::to_string)
    })
}

/// The one-line overview for a stretch of conversation: the first real sentence
/// of the message that started it.
fn interstitial_overview(block: &TurnBlock<'_>) -> String {
    let source = user_message(block)
        .or_else(|| assistant_message(block))
        .unwrap_or_default();
    let summary = first_sentence(&source);
    if summary.is_empty() {
        "conversation".to_string()
    } else {
        summary
    }
}

/// Strip the machinery that leads a resumed turn — the `[Fri 09:12 PDT — resumed]`
/// clock stamp and any system-reminder block — then take the first sentence.
fn first_sentence(content: &str) -> String {
    let mut body = content.trim();
    while body.starts_with('[') {
        match body.find(']') {
            Some(end) => body = body[end + 1..].trim_start(),
            None => break,
        }
    }
    if let Some(start) = body.find("<system-reminder>") {
        let tail = body[start..]
            .find("</system-reminder>")
            .map(|end| &body[start + end + "</system-reminder>".len()..])
            .unwrap_or("");
        body = tail.trim_start();
    }
    let body = body.trim_start_matches(['#', '*', '-', ' ']).trim();
    let end = body
        .find(['.', '!', '?', '\n'])
        .map(|index| index + 1)
        .unwrap_or(body.len());
    one_line(body[..end].trim_end_matches(['.', '\n']))
}

/// Collapse to a single trimmed line, capped so one chapter cannot outweigh the
/// range it stands in for.
fn one_line(value: &str) -> String {
    const MAX: usize = 120;
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= MAX {
        return collapsed;
    }
    let truncated: String = collapsed.chars().take(MAX).collect();
    format!("{}…", truncated.trim_end())
}

// ── Coordinates and children ────────────────────────────────────────────────

struct NodeCoordinates {
    base_uri: String,
    project_key: String,
    number: i32,
    exec_seq: i32,
    label: String,
    status: String,
}

/// Resolve the node's canonical address. Chapter URIs are only worth writing if
/// they resolve, so a job Cairn cannot address is a composition failure rather
/// than a seed full of dead links.
async fn node_coordinates(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Option<NodeCoordinates> {
    let base_uri = crate::jobs::queries::home_uri_for_job_conn(conn, job_id)
        .await
        .ok()
        .flatten()?;
    let mut rows = conn
        .query(
            "SELECT p.key, i.number, COALESCE(e.seq, 1), COALESCE(j.node_name, j.uri_segment), j.status
             FROM jobs j
             JOIN issues i ON i.id = j.issue_id
             JOIN projects p ON p.id = i.project_id
             LEFT JOIN executions e ON e.id = j.execution_id
             WHERE j.id = ?1 LIMIT 1",
            params![job_id],
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    Some(NodeCoordinates {
        base_uri,
        project_key: row.text(0).ok()?,
        number: row.i64(1).ok()? as i32,
        exec_seq: row.i64(2).ok()? as i32,
        label: row
            .opt_text(3)
            .ok()?
            .unwrap_or_else(|| "thread".to_string()),
        status: row.text(4).ok()?,
    })
}

/// Where one child issue stands at composition time.
///
/// Every field is read live. None of it is new state: it is the same data an
/// issue read returns, assembled into one line so a resumed thread does not have
/// to fetch five issues to learn what Cairn already knows.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChildStatus {
    uri: String,
    title: String,
    status: String,
    /// The newest execution's top-level nodes, in creation order.
    nodes: Vec<(String, String)>,
    pr: Option<(i64, String)>,
    /// The newest verdict per check name across this child's jobs.
    checks: Vec<(String, String)>,
    unanswered_questions: i64,
    last_activity_at: Option<i64>,
}

/// Statuses that end an issue. A child in one of these is history: its turns may
/// be compacted into a chapter, and its census line carries no live detail.
const TERMINAL_ISSUE_STATUSES: [&str; 3] = ["merged", "closed", "failed"];

impl ChildStatus {
    fn is_open(&self) -> bool {
        !TERMINAL_ISSUE_STATUSES.contains(&self.status.as_str())
    }

    /// The facets of a live child, in the order a thread asks about them: who is
    /// working it, whether a PR is out, whether the checks are green, whether it
    /// is blocked on the user, and when it last moved. An absent facet is
    /// omitted rather than rendered empty — "no PR" is not news.
    fn detail_line(&self, now: i64) -> String {
        let mut facets: Vec<String> = Vec::new();
        if !self.nodes.is_empty() {
            facets.push(
                self.nodes
                    .iter()
                    .map(|(name, status)| format!("{name} {}", job_status_icon(status)))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }
        if let Some((number, status)) = &self.pr {
            facets.push(format!("PR #{number} {status}"));
        }
        if let Some(checks) = self.check_summary() {
            facets.push(checks);
        }
        match self.unanswered_questions {
            0 => {}
            1 => facets.push("1 unanswered question".to_string()),
            count => facets.push(format!("{count} unanswered questions")),
        }
        if let Some(at) = self.last_activity_at {
            let elapsed = now.saturating_sub(at);
            facets.push(match elapsed >= 60 {
                true => format!("active {} ago", crate::clock::format_elapsed(elapsed)),
                // Sub-minute precision is noise here for the same reason it is at
                // a turn boundary: it implies accuracy nothing acts on.
                false => "active just now".to_string(),
            });
        }
        facets.join(" · ")
    }

    /// Check state as a count plus the names that are red, because the count
    /// alone tells a thread something is wrong without telling it what.
    fn check_summary(&self) -> Option<String> {
        if self.checks.is_empty() {
            return None;
        }
        let failing: Vec<&str> = self
            .checks
            .iter()
            .filter(|(_, verdict)| verdict != "passed")
            .map(|(name, _)| name.as_str())
            .collect();
        if failing.is_empty() {
            return Some(format!("checks {}✓", self.checks.len()));
        }
        Some(format!(
            "checks {}✓ {}✗ ({})",
            self.checks.len() - failing.len(),
            failing.len(),
            failing.join(", ")
        ))
    }
}

/// Render the census. Open children carry their full detail line; a terminal
/// child collapses to one line, because its detail is history and the table of
/// contents below already addresses it. Without that asymmetry a thread with
/// forty merged children would pay two lines each, forever, for state nothing
/// acts on.
fn render_children(children: &[ChildStatus], now: i64) -> String {
    if children.is_empty() {
        return String::new();
    }
    let mut out = String::from("## Children\n\n");
    out.push_str(CHILDREN_NOTICE);
    out.push_str("\n\n");
    for child in children {
        out.push_str(&format!(
            "- `{}` **{}** · {}\n",
            child.uri,
            child.status,
            one_line(&child.title)
        ));
        if !child.is_open() {
            continue;
        }
        let detail = child.detail_line(now);
        if !detail.is_empty() {
            out.push_str(&format!("  {detail}\n"));
        }
    }
    out.push('\n');
    out
}

/// Assemble every child issue's live status.
///
/// Six queries, each keyed on the parent issue rather than run per child, so
/// composition costs the same whether a thread has three children or forty.
///
/// Enumerating the children is strict: a census that silently omits one is the
/// staleness this block exists to remove. Enriching them is best-effort — a
/// facet that cannot be read is left off the line, which is what the renderer
/// does for a facet that simply does not exist.
async fn load_children(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Result<Vec<ChildStatus>, String> {
    let mut children: Vec<ChildStatus> = Vec::new();
    let mut at: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    let mut rows = conn
        .query(
            "SELECT child.id, p.key, child.number, child.title, child.status
             FROM issues child
             JOIN projects p ON p.id = child.project_id
             WHERE child.parent_issue_id = (SELECT issue_id FROM jobs WHERE id = ?1)
             ORDER BY child.number ASC",
            params![job_id],
        )
        .await
        .map_err(|error| error.to_string())?;
    while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
        let id = row.text(0).map_err(|error| error.to_string())?;
        let key = row.text(1).map_err(|error| error.to_string())?;
        let number = row.i64(2).map_err(|error| error.to_string())? as i32;
        at.insert(id, children.len());
        children.push(ChildStatus {
            uri: cairn_common::uri::build_issue_uri(&key, number),
            title: row.text(3).map_err(|error| error.to_string())?,
            status: row.text(4).map_err(|error| error.to_string())?,
            nodes: Vec::new(),
            pr: None,
            checks: Vec::new(),
            unanswered_questions: 0,
            last_activity_at: None,
        });
    }
    if children.is_empty() {
        return Ok(children);
    }

    // The newest execution's top-level nodes. An older execution's nodes describe
    // an attempt that has been superseded, which would read as live work.
    if let Ok(mut rows) = conn
        .query(
            "SELECT j.issue_id, COALESCE(j.node_name, j.uri_segment), j.status
             FROM jobs j
             JOIN executions e ON e.id = j.execution_id
             JOIN issues child ON child.id = j.issue_id
             WHERE child.parent_issue_id = (SELECT issue_id FROM jobs WHERE id = ?1)
               AND j.parent_job_id IS NULL
               AND e.seq = (SELECT MAX(latest.seq) FROM executions latest WHERE latest.issue_id = j.issue_id)
             ORDER BY j.created_at ASC",
            params![job_id],
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(issue_id), Ok(Some(name)), Ok(status)) =
                (row.text(0), row.opt_text(1), row.text(2))
            else {
                continue;
            };
            if let Some(child) = at.get(&issue_id).map(|index| &mut children[*index]) {
                child.nodes.push((name, status));
            }
        }
    }

    // Oldest first, so the last write per child is its newest pull request. A
    // non-positive number is a phantom binding, not a pull request.
    if let Ok(mut rows) = conn
        .query(
            "SELECT m.issue_id, m.github_pr_number, m.status
             FROM merge_requests m
             JOIN issues child ON child.id = m.issue_id
             WHERE child.parent_issue_id = (SELECT issue_id FROM jobs WHERE id = ?1)
               AND m.github_pr_number > 0
             ORDER BY m.opened_at ASC",
            params![job_id],
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(issue_id), Ok(Some(number)), Ok(status)) =
                (row.text(0), row.opt_i64(1), row.text(2))
            else {
                continue;
            };
            if let Some(child) = at.get(&issue_id).map(|index| &mut children[*index]) {
                child.pr = Some((number, status));
            }
        }
    }

    if let Ok(mut rows) = conn
        .query(
            "SELECT r.issue_id, COUNT(*)
             FROM prompts p
             JOIN runs r ON r.id = p.run_id
             JOIN issues child ON child.id = r.issue_id
             WHERE child.parent_issue_id = (SELECT issue_id FROM jobs WHERE id = ?1)
               AND p.answered_at IS NULL
             GROUP BY r.issue_id",
            params![job_id],
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(issue_id), Ok(count)) = (row.text(0), row.i64(1)) else {
                continue;
            };
            if let Some(child) = at.get(&issue_id).map(|index| &mut children[*index]) {
                child.unanswered_questions = count;
            }
        }
    }

    // Oldest first again: the newest observation of a check name replaces the
    // one before it, so each child ends with one current verdict per check.
    if let Ok(mut rows) = conn
        .query(
            "SELECT j.issue_id, o.check_name, o.verdict
             FROM check_result_observations o
             JOIN jobs j ON j.id = o.job_id
             JOIN issues child ON child.id = j.issue_id
             WHERE child.parent_issue_id = (SELECT issue_id FROM jobs WHERE id = ?1)
             ORDER BY o.ran_at ASC",
            params![job_id],
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(issue_id), Ok(name), Ok(verdict)) = (row.text(0), row.text(1), row.text(2))
            else {
                continue;
            };
            let Some(child) = at.get(&issue_id).map(|index| &mut children[*index]) else {
                continue;
            };
            match child.checks.iter_mut().find(|(known, _)| *known == name) {
                Some(entry) => entry.1 = verdict,
                None => child.checks.push((name, verdict)),
            }
        }
    }

    // Last activity is the newest event under the child, not `issues.updated_at`:
    // patching an issue's labels is not the work moving.
    if let Ok(mut rows) = conn
        .query(
            "SELECT r.issue_id, MAX(e.timestamp)
             FROM events e
             JOIN runs r ON r.id = e.run_id
             JOIN issues child ON child.id = r.issue_id
             WHERE child.parent_issue_id = (SELECT issue_id FROM jobs WHERE id = ?1)
             GROUP BY r.issue_id",
            params![job_id],
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(issue_id), Ok(Some(timestamp))) = (row.text(0), row.opt_i64(1)) else {
                continue;
            };
            if let Some(child) = at.get(&issue_id).map(|index| &mut children[*index]) {
                child.last_activity_at = Some(timestamp);
            }
        }
    }

    Ok(children)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    const THREAD_JOB: &str = "job-thread";
    const DONE_CHILD: &str = "cairn://p/PRJ/20";
    const OPEN_CHILD: &str = "cairn://p/PRJ/21";

    /// Composition time for every test. The fixture's newest event is at 141, so
    /// a child's last activity reads as a couple of hours old.
    const NOW: i64 = 10_000;

    /// A thread with fourteen turns: four old ones behind the recency window,
    /// then ten recent. Of the four old turns, one is plain conversation, two
    /// discuss a child that has since merged, and one touches a child that is
    /// still open.
    async fn thread_fixture(name: &str) -> LocalDb {
        let db = migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','PRJ','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i-thread','p',1,'Platform thread','active','none',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at, parent_issue_id)
               VALUES ('i-done','p',20,'Running panel for the executor fleet','merged','none',1,1,'i-thread');
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at, parent_issue_id)
               VALUES ('i-open','p',21,'Lease renewal backoff','active','none',1,1,'i-thread');
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e','recipe','i-thread','p','running',1,1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, current_session_id, created_at, updated_at)
               VALUES ('job-thread','e','i-thread','p','running','thread','thread','sess-1',1,1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('sess-1','job-thread','claude','open',1,1,1);
             INSERT INTO runs(id, job_id, issue_id, status, created_at, updated_at)
               VALUES ('run-1','job-thread','i-thread','running',1,1);",
        )
        .await
        .unwrap();

        // Turn 1: plain conversation, no child in it.
        turn(&db, 1, "Shape of the executor protocol. We should settle on lease-based residency before anything else.", None).await;
        // Turns 2 and 3: one merged child, worked across two turns.
        turn(
            &db,
            2,
            &format!("Please file the running panel as {DONE_CHILD} and get it moving."),
            Some("MERGED_CHILD_TURN_TWO"),
        )
        .await;
        turn(
            &db,
            3,
            &format!("Checking on {DONE_CHILD} now."),
            Some("MERGED_CHILD_TURN_THREE"),
        )
        .await;
        // Turn 4: touches a child that has NOT resolved.
        turn(
            &db,
            4,
            &format!("Backoff for lease renewal is still open at {OPEN_CHILD}."),
            Some("OPEN_CHILD_TURN_FOUR"),
        )
        .await;
        for sequence in 5..=14 {
            turn(
                &db,
                sequence,
                &format!("recent turn {sequence}"),
                Some("RECENT_BODY"),
            )
            .await;
        }
        db
    }

    async fn turn(db: &LocalDb, sequence: i64, user: &str, assistant: Option<&str>) {
        let turn_id = format!("turn-{sequence}");
        db.execute(
            "INSERT INTO turns(id, session_id, run_id, job_id, sequence, created_at, updated_at)
             VALUES (?1,'sess-1','run-1','job-thread',?2,?2,?2)",
            params![turn_id.as_str(), sequence],
        )
        .await
        .unwrap();
        insert_event(
            db,
            &format!("ev-{sequence}-user"),
            sequence * 10,
            "user",
            &serde_json::json!({ "content": user }).to_string(),
            &turn_id,
        )
        .await;
        if let Some(assistant) = assistant {
            insert_event(
                db,
                &format!("ev-{sequence}-assistant"),
                sequence * 10 + 1,
                "assistant",
                &serde_json::json!({ "content": assistant }).to_string(),
                &turn_id,
            )
            .await;
        }
    }

    async fn insert_event(
        db: &LocalDb,
        id: &str,
        sequence: i64,
        event_type: &str,
        data: &str,
        turn_id: &str,
    ) {
        db.execute(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES (?1,'run-1',?2,?2,?3,?4,?2,?5)",
            params![id, sequence, event_type, data, turn_id],
        )
        .await
        .unwrap();
    }

    /// Move the job onto a successor session, which is what makes a persisted
    /// generation applied.
    async fn rotate_session(db: &LocalDb, session_id: &str) {
        db.execute(
            "INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
             VALUES (?1,'job-thread','claude','open',2,1,1)",
            params![session_id],
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE jobs SET current_session_id = ?1 WHERE id = 'job-thread'",
            params![session_id],
        )
        .await
        .unwrap();
    }

    async fn mark_done_child(db: &LocalDb) {
        compaction::mark_child_terminal(
            db,
            THREAD_JOB,
            &ChildMark {
                child_issue_id: "i-done".to_string(),
                child_issue_uri: DONE_CHILD.to_string(),
                child_title: "Running panel for the executor fleet".to_string(),
                final_status: "merged".to_string(),
                marked_at: 500,
            },
        )
        .await
        .unwrap();
    }

    async fn write_arc(db: &LocalDb, version: i64, intent: &str) {
        let data = serde_json::json!({
            "current_intent": intent,
            "rulings": [{
                "text": "Do not rebuild the fleet view",
                "status": "rejected",
                "rationale": "premature until residency leases settle",
                "provenance": ["cairn://p/PRJ/20"]
            }],
            "open_questions": [{ "question": "Who owns lease renewal?" }]
        })
        .to_string();
        db.execute(
            "INSERT INTO artifacts(id, job_id, artifact_type, schema_version, data, version, output_name, created_at, updated_at, confirmed)
             VALUES (?1,'job-thread','context-self',1,?2,?3,'arc',?3,?3,1)",
            params![format!("arc-{version}"), data, version],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_composed_seed_keeps_the_arc_the_open_child_and_ten_turns() {
        let db = thread_fixture("thread-seed-compose.db").await;
        mark_done_child(&db).await;
        write_arc(&db, 1, "Getting the fleet onto lease-based residency.").await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        // The arc is copied, not summarized: every authored string survives.
        assert!(seed
            .content
            .contains("Getting the fleet onto lease-based residency."));
        assert!(seed.content.contains("Do not rebuild the fleet view"));
        assert!(seed
            .content
            .contains("premature until residency leases settle"));
        assert!(seed.content.contains("**rejected**"));
        assert!(seed.content.contains("Who owns lease renewal?"));

        // The merged child became one chapter: title as overview, its own URI as
        // the address, and its turns are gone from the body.
        let child = seed
            .entries
            .iter()
            .find(|entry| entry.source == EntrySource::Child)
            .expect("a child chapter");
        assert_eq!(child.overview, "Running panel for the executor fleet");
        assert_eq!(child.content_uri, DONE_CHILD);
        assert_eq!((child.start_block, child.end_block), (1, 2));
        assert!(!seed.content.contains("MERGED_CHILD_TURN_TWO"));
        assert!(!seed.content.contains("MERGED_CHILD_TURN_THREE"));

        // The turn touching an unresolved child is never dropped, even though it
        // sits behind the recency window.
        assert!(
            seed.content.contains("OPEN_CHILD_TURN_FOUR"),
            "a turn referencing an open child was compacted away: {}",
            seed.content
        );

        // Interstitial conversation keeps an overview AND an address.
        let interstitial = seed
            .entries
            .iter()
            .find(|entry| entry.source == EntrySource::Interstitial)
            .expect("an interstitial chapter");
        assert_eq!(interstitial.overview, "Shape of the executor protocol");
        assert!(
            interstitial.content_uri.contains("/chat?offset=0&limit=1"),
            "interstitial chapter is not re-readable: {}",
            interstitial.content_uri
        );

        assert_eq!(seed.consumed_child_issue_ids, vec!["i-done".to_string()]);
        assert_eq!(seed.recency_start_block, 4);
        assert!(seed.source_bytes > seed.candidate_bytes);

        // The table of contents and the verbatim section share one numbering:
        // chapters cover turns 1–3, the kept turns run 4–14.
        assert!(
            seed.content.contains("- turn 1 ·") && seed.content.contains("- turns 2–3 ·"),
            "chapter ranges are not turn-numbered: {}",
            seed.content
        );
        assert!(
            seed.content.contains("## Turn 4 ·") && seed.content.contains("## Turn 14 ·"),
            "kept turns are numbered relative to the subset instead of the thread: {}",
            seed.content
        );
    }

    /// Give the open child everything a live child accumulates: a running
    /// execution, a pull request, a red check beside green ones, an unanswered
    /// question, and recent activity.
    async fn make_open_child_live(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e-open-old','build','i-open','p','complete',1,1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e-open','build','i-open','p','running',2,2);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('job-open-stale','e-open-old','i-open','p','failed','builder','builder',1,1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('job-open-build','e-open','i-open','p','complete','builder','builder',2,2);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('job-open-review','e-open','i-open','p','running','review','review',3,3);
             INSERT INTO runs(id, job_id, issue_id, status, created_at, updated_at)
               VALUES ('run-open','job-open-build','i-open','running',1,1);
             INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, github_pr_number)
               VALUES ('mr-open','job-open-build','p','i-open','Lease renewal backoff','feat/lease','main','open',10,10,892);
             INSERT INTO prompts(id, run_id, questions, created_at)
               VALUES ('q-open','run-open','[{\"question\":\"which backoff curve?\"}]',20);
             INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
               VALUES ('ev-open','run-open',1,9_100,'assistant','{}',9_100);",
        )
        .await
        .unwrap();
        for (name, verdict) in [
            ("rust-tests", "passed"),
            ("lint", "passed"),
            ("lint", "failed"),
        ] {
            observe_check(db, name, verdict).await;
        }
    }

    /// One check observation against the open child's build node. Written with an
    /// id derived from an incrementing clock so the newest verdict per check name
    /// is unambiguous.
    async fn observe_check(db: &LocalDb, check_name: &str, verdict: &str) {
        static RAN_AT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(100);
        let ran_at = RAN_AT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        db.execute(
            "INSERT INTO check_result_observations(
                 id, project_id, commit_sha, tree_hash, check_name, input_hash,
                 environment_fingerprint, exit_code, verdict, complete, reusable,
                 parser_version, result_schema_version, ran_at, duration_ms, job_id,
                 cadence, output_tail
             ) VALUES (?1,'p','sha','tree',?2,'input','env',0,?3,1,1,1,1,?4,1,'job-open-build','write','')",
            params![format!("obs-{ran_at}"), check_name, verdict, ran_at],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn the_census_reports_a_live_child_and_collapses_a_finished_one() {
        let db = thread_fixture("thread-seed-children.db").await;
        mark_done_child(&db).await;
        make_open_child_live(&db).await;
        write_arc(&db, 1, "Getting the fleet onto lease-based residency.").await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        // The live child carries every facet a thread would otherwise re-read it
        // to learn, and reads its nodes from the NEWEST execution: the failed
        // builder of the superseded attempt is not where this work stands.
        assert!(
            seed.content.contains(
                "- `cairn://p/PRJ/21` **active** · Lease renewal backoff\n  \
                 builder ✓ review ◐ · PR #892 open · checks 1✓ 1✗ (lint) · \
                 1 unanswered question · active 15m ago\n"
            ),
            "the live child's census line is wrong: {}",
            seed.content
        );

        // A finished child collapses to one line. Its detail is history, and the
        // table of contents below already addresses it.
        assert!(
            seed.content.contains(
                "- `cairn://p/PRJ/20` **merged** · Running panel for the executor fleet\n"
            ),
            "the merged child's census line is wrong: {}",
            seed.content
        );

        // The census sits between authored intent and generated history.
        let arc = seed.content.find("## Arc").expect("an arc section");
        let children = seed
            .content
            .find("## Children")
            .expect("a children section");
        let history = seed.content.find("## History").expect("a history section");
        assert!(arc < children && children < history);
    }

    #[tokio::test]
    async fn the_census_contradicts_a_stale_arc_rather_than_echoing_it() {
        // The defect this block exists for: a thread inherits its arc verbatim,
        // so a census authored into `current_intent` is stale by construction.
        // The generated lines must state live status regardless of what the arc
        // claims, and both must survive — a thread that only saw the arc's
        // version would act on it.
        let db = thread_fixture("thread-seed-children-vs-arc.db").await;
        make_open_child_live(&db).await;
        write_arc(
            &db,
            1,
            "21 is still unstarted and has no PR; nothing is waiting on me.",
        )
        .await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        assert!(seed.content.contains("21 is still unstarted and has no PR"));
        assert!(
            seed.content.contains("PR #892 open"),
            "the census deferred to the arc's stale claim: {}",
            seed.content
        );
        assert!(seed.content.contains("1 unanswered question"));
    }

    #[tokio::test]
    async fn a_thread_with_no_children_carries_no_census() {
        // An empty heading is not information, and this seed is composed on every
        // continuation of every thread.
        let db = migrated_test_db("thread-seed-childless.db").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','PRJ','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i-thread','p',1,'Platform thread','active','none',1,1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e','recipe','i-thread','p','running',1,1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, current_session_id, created_at, updated_at)
               VALUES ('job-thread','e','i-thread','p','running','thread','thread','sess-1',1,1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('sess-1','job-thread','claude','open',1,1,1);
             INSERT INTO runs(id, job_id, issue_id, status, created_at, updated_at)
               VALUES ('run-1','job-thread','i-thread','running',1,1);",
        )
        .await
        .unwrap();
        turn(&db, 1, "just a conversation", Some("fine")).await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        assert!(!seed.content.contains("## Children"));
    }

    #[tokio::test]
    async fn an_unmarked_child_is_never_folded_into_a_chapter() {
        // Without the terminal mark, the same turns are conversation. They may be
        // compacted as interstitial chapters, but never attributed to the child.
        let db = thread_fixture("thread-seed-unmarked.db").await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        assert!(seed
            .entries
            .iter()
            .all(|entry| entry.source == EntrySource::Interstitial));
        assert!(seed.consumed_child_issue_ids.is_empty());
    }

    #[tokio::test]
    async fn a_thread_with_no_arc_is_told_so_rather_than_given_a_generated_one() {
        let db = thread_fixture("thread-seed-no-arc.db").await;
        mark_done_child(&db).await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        assert!(
            seed.content
                .contains("No arc has been authored for this thread yet"),
            "the missing-arc notice is the only substitute for an authored arc"
        );
        // The mechanical half still works: a missing arc is a soft dependency.
        assert!(seed.content.contains(DONE_CHILD));
    }

    #[tokio::test]
    async fn the_arc_changes_the_seed_without_changing_the_chapters() {
        // Authored intent and generated chapters are produced by different
        // things, so editing one must not move the other.
        let db = thread_fixture("thread-seed-arc-independence.db").await;
        mark_done_child(&db).await;
        write_arc(&db, 1, "FIRST INTENT").await;
        let first = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        write_arc(&db, 2, "SECOND INTENT").await;
        let second = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        assert!(first.content.contains("FIRST INTENT"));
        assert!(second.content.contains("SECOND INTENT"));
        assert!(!second.content.contains("FIRST INTENT"));
        assert_eq!(first.entries, second.entries);
    }

    #[tokio::test]
    async fn a_prior_table_of_contents_is_carried_forward_not_regenerated() {
        let db = thread_fixture("thread-seed-carry-forward.db").await;
        mark_done_child(&db).await;
        let first = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();
        compaction::persist_generation(
            &db,
            THREAD_JOB,
            &compaction::AppliedCompaction {
                trigger: compaction::CompactionTrigger::Expiry,
                source_session_id: first.source_session_id.clone(),
                entries: first.entries.clone(),
                seed_bytes: first.content.len() as i64,
                source_bytes: first.source_bytes,
                candidate_bytes: first.candidate_bytes,
                compacted_through_block: first.compacted_through_block,
                recency_start_block: first.recency_start_block,
                consumed_child_issue_ids: first.consumed_child_issue_ids.clone(),
            },
            1_000,
        )
        .await
        .unwrap();
        rotate_session(&db, "sess-2").await;

        let second = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        assert_eq!(
            second.entries, first.entries,
            "a second composition over the same transcript must reach the same table of contents"
        );
        assert_eq!(
            second.new_entries, 0,
            "turns already standing behind a chapter were compacted a second time"
        );
        assert!(
            second.content.contains(DONE_CHILD),
            "a carried-forward chapter vanished from the seed"
        );
    }

    #[tokio::test]
    async fn a_seed_whose_rotation_failed_recomposes_identically_while_still_warm() {
        // The end-to-end shape of the retry guarantee: persistence happens
        // before rotation, so if the rotation fails the job is still on the same
        // session. The next continuation must compose exactly what the first one
        // did — same chapters, same folded marks, same size measurement — or the
        // size trigger can never select again and the thread carries its whole
        // live prefix until the one-hour expiry.
        let db = thread_fixture("thread-seed-retry-while-warm.db").await;
        mark_done_child(&db).await;
        let first = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();
        compaction::persist_generation(
            &db,
            THREAD_JOB,
            &compaction::AppliedCompaction {
                trigger: compaction::CompactionTrigger::Capacity,
                source_session_id: first.source_session_id.clone(),
                entries: first.entries.clone(),
                seed_bytes: first.content.len() as i64,
                source_bytes: first.source_bytes,
                candidate_bytes: first.candidate_bytes,
                compacted_through_block: first.compacted_through_block,
                recency_start_block: first.recency_start_block,
                consumed_child_issue_ids: first.consumed_child_issue_ids.clone(),
            },
            1_000,
        )
        .await
        .unwrap();
        // No rotation: the job stays on the session the seed was composed from.

        let retry = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        assert_eq!(retry.entries, first.entries);
        assert_eq!(retry.new_entries, first.new_entries);
        assert_eq!(
            retry.consumed_child_issue_ids, first.consumed_child_issue_ids,
            "the terminal child stopped counting toward the size trigger"
        );
        assert_eq!(
            (retry.source_bytes, retry.candidate_bytes),
            (first.source_bytes, first.candidate_bytes),
            "the prefix the trigger prices changed without a rotation"
        );
        assert_eq!(retry.content, first.content);
    }

    #[tokio::test]
    async fn a_nested_seed_event_never_re_expands() {
        // A thread reseeds over and over, so a seed's body must not reappear
        // inside the next seed; otherwise every compaction grows the one before.
        let db = thread_fixture("thread-seed-nested.db").await;
        insert_event(
            &db,
            "ev-seed",
            145,
            "user:seed",
            &serde_json::json!({ "content": "HEADER\n\nEMBEDDED_PRIOR_SEED_BODY" }).to_string(),
            "turn-14",
        )
        .await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();

        assert!(!seed.content.contains("EMBEDDED_PRIOR_SEED_BODY"));
        assert!(seed.content.contains("[prior context compacted]"));
    }

    #[tokio::test]
    async fn a_job_with_no_transcript_cannot_compose() {
        let db = migrated_test_db("thread-seed-empty.db").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','PRJ','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i-thread','p',1,'Platform thread','active','none',1,1);
             INSERT INTO jobs(id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('job-thread','i-thread','p','running','thread','thread',1,1);",
        )
        .await
        .unwrap();

        assert!(compose_thread_seed(&db, THREAD_JOB, NOW).await.is_err());
    }

    /// The compactor reads one named artifact, and the thread recipe is what
    /// creates it. Nothing else connects those two slices, and the failure mode
    /// if they drift is silent rather than loud: a renamed artifact node does
    /// not error anywhere, it just means every composed seed reports that no arc
    /// was authored, forever. That is the whole top preservation tier going
    /// missing without a single failing test — so pin the name here, against the
    /// recipe that actually ships.
    #[test]
    fn the_shipped_thread_recipe_declares_the_arc_this_composer_reads() {
        let recipe =
            crate::models::RecipeFile::from_yaml(include_str!("../../../../recipes/thread.yaml"))
                .expect("the bundled thread recipe parses")
                .into_recipe(Some("default".to_string()), None);

        let arc = recipe
            .nodes
            .iter()
            .filter_map(|node| node.artifact_config.as_ref())
            .find(|config| config.name == ARC_ARTIFACT_NAME)
            .unwrap_or_else(|| {
                panic!(
                    "the thread recipe declares no `{ARC_ARTIFACT_NAME}` artifact, so every \
                     composed seed would silently report an unauthored arc"
                )
            });

        // The schema is the read contract: the provenance-first shape this
        // composer renders, with provenance required on every ruling.
        let schema = arc.schema.as_ref().expect("the arc carries a schema");
        let properties = schema
            .get("properties")
            .and_then(|value| value.as_object())
            .expect("the arc schema declares properties");
        for field in ["current_intent", "rulings", "open_questions"] {
            assert!(
                properties.contains_key(field),
                "the arc schema dropped `{field}`, which this composer renders"
            );
        }
        let ruling_required = properties["rulings"]["items"]["required"]
            .as_array()
            .expect("a ruling declares required fields");
        for field in ["text", "status", "rationale", "provenance"] {
            assert!(
                ruling_required
                    .iter()
                    .any(|value| value.as_str() == Some(field)),
                "a ruling no longer requires `{field}` — an unprovenanced ruling gets relitigated"
            );
        }
    }

    #[test]
    fn an_issue_uri_is_not_matched_inside_a_longer_one() {
        // `cairn://p/PRJ/33` sits inside `cairn://p/PRJ/339`, and attributing one
        // child's turns to another would compact the wrong range.
        assert!(mentions_uri(
            "work on cairn://p/PRJ/33 today",
            "cairn://p/PRJ/33"
        ));
        assert!(mentions_uri(
            "see cairn://p/PRJ/33/1/builder",
            "cairn://p/PRJ/33"
        ));
        assert!(!mentions_uri(
            "work on cairn://p/PRJ/339",
            "cairn://p/PRJ/33"
        ));
    }

    #[test]
    fn an_overview_skips_the_clock_stamp_and_the_system_reminder() {
        let content = "[Fri 23:19 PST — resumed after 1m]\n\n<system-reminder>noise</system-reminder>\nLand the residency lease change. Then look at the panel.";
        assert_eq!(first_sentence(content), "Land the residency lease change");
    }

    #[test]
    fn an_arc_in_an_unrecognized_shape_is_passed_through_whole() {
        // The compactor is not allowed to decide that authored intent it does not
        // recognize was unimportant.
        let data = serde_json::json!({ "content": "free-form arc the thread wrote" });
        assert_eq!(render_arc_data(&data), "free-form arc the thread wrote");
    }

    #[test]
    fn a_ruling_without_provenance_says_so() {
        // An unprovenanced ruling gets relitigated; render the absence rather
        // than a confident bare claim.
        let data = serde_json::json!({
            "rulings": [{ "text": "Park the migration", "status": "accepted" }]
        });
        let rendered = render_arc_data(&data);
        assert!(rendered.contains("Park the migration"));
        assert!(rendered.contains("provenance: none recorded"));
    }
}
