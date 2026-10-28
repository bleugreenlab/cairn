//! Composing the seed a rebuilt session resumes from: a thread's (CAIRN-3388)
//! and, at the end of this module, any other node's.
//!
//! A full-fidelity digest of the whole transcript gets more expensive with every
//! turn while carrying less of what matters, and a session seeded with one near
//! its window's trigger rotates again before it has done anything. So every
//! seed is held to one token budget (`seed_budget_tokens`, measured by
//! [`seed_tokens`]). The thread composer bounds every generated part of a
//! rebuilt seed while leaving the authored arc verbatim and budget-exempt. It
//! renders, in order:
//!
//! 1. the authored arc;
//! 2. a live, bounded children census;
//! 3. recent persisted era summaries with an address to older eras; and
//! 4. a V5-token-budgeted verbatim selection with an unconditional recency floor,
//!    bounded unresolved-child priority, and addressable markers for omitted runs.
//!
//! Two of those rules are richer than a budget can promise on its own: every
//! open child renders in full, and the newest turns survive whatever they weigh.
//! Either input, large enough, exceeds the whole budget before any arithmetic
//! runs. So the bound is made true at final composition, where an oversized body
//! is recomposed under an inverted priority — the newest turn at the center,
//! every child collapsed to the line that addresses it, and a tokenizer cut as
//! the last boundary. The authored arc stays verbatim and exempt.
//!
//! Chapters remain internal natural fold boundaries. They never render, and eras
//! replace their lifetime accumulation with generation-independent summaries.
//! Nothing here decides *when* to rotate; that remains the lifecycle trigger's job.

use cairn_db::turso::params;
use cairn_tokenize::{count, Family};

use std::collections::{HashMap, HashSet};

use crate::storage::{LocalDb, RowExt};
use crate::tenets::{TenetOrigin, TenetStatus, TenetView};
use crate::threads::arc_store::ArcProjection;
use crate::threads::compaction::{self, ChildMark, EntrySource, TocEntry};

use super::common::{connect_for_read, job_status_icon};
use super::transcript::DigestCoordinate;
use super::transcript::{
    format_transcript_digest_with, group_turn_blocks, load_job_events_ordered, DigestMeta,
    EventRow, TurnBlock,
};
use cairn_models::continuation::DigestOptions;

/// How many turns survive verbatim. Compaction is worth doing hard or not at
/// all — halving a prefix takes ~20 turns to pay back its cache write, cutting
/// it to a tenth pays back in ~3 — and children are re-fetchable, which is what
/// makes cutting hard safe.
const RECENCY_FLOOR_TURNS: usize = 3;
const UNRESOLVED_CHILD_SHARE: usize = 50;
const ERA_MIN_SOURCE_TOKENS: usize = 8_000;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SeedPolicy {
    pub budget_tokens: usize,
    pub inline_eras: usize,
}

impl Default for SeedPolicy {
    fn default() -> Self {
        Self {
            budget_tokens: 12_000,
            inline_eras: 3,
        }
    }
}

pub(crate) fn seed_tokens(text: &str) -> usize {
    // One deterministic family makes the configured bound stable even when the
    // consuming model changes. Precision across model tokenizers is not the goal.
    count(text, Family::V5) as usize
}

/// Where to end `text` so that `seed_tokens` measures the prefix at or under
/// `budget`. The returned offset is a UTF-8 boundary in the source, so the
/// prefix is directly sliceable.
///
/// `cut` budgets content tokens and deliberately excludes message framing,
/// while `seed_tokens` counts a string as a message and charges that framing on
/// top. Charging the framing before cutting gets close, but tokenization is not
/// additive at that boundary, so the chosen prefix is measured and retreated
/// until it actually fits. A budget below the framing itself cannot be met by any
/// string, the empty one included.
fn seed_cut(text: &str, budget: usize) -> usize {
    let mut content = budget.saturating_sub(seed_tokens(""));
    loop {
        let offset = cairn_tokenize::cut(text, content.min(u32::MAX as usize) as u32, Family::V5)
            .byte_offset;
        let tokens = seed_tokens(&text[..offset]);
        if tokens <= budget || offset == 0 {
            return offset;
        }
        content = content.saturating_sub((tokens - budget).max(1));
    }
}

/// The artifact a thread authors its arc into.
const ARC_ARTIFACT_NAME: &str = "arc";

const ARC_MISSING_NOTICE: &str = "_No arc has been authored for this thread yet. Write one to `cairn:~/arc` — your current intent and the work in motion, each with the URI it came from. Decisions are tenets, proposed at `cairn:@/tenets`; open questions go in `cairn:~/db`. The arc is the only part of this context that cannot be regenerated from the transcript, so nothing else in this seed substitutes for it._";

const NO_HISTORY_NOTICE: &str =
    "_Nothing has been compacted yet; the whole session so far is below._";

/// Framing for the children census. It says where the lines came from, because
/// a resumed thread that reads them as prose it once wrote will re-verify them.
const CHILDREN_NOTICE: &str = "_Read from live state as this seed was composed — not authored, not inherited. Trust it over anything the arc says about where a child stands, and re-read a child only when you need detail this line does not carry._";

/// The census's framing once the budget forces it to collapse. It keeps the
/// provenance claim in a fraction of the tokens and adds the one fact only a
/// degraded seed carries: the detail is not here, the address for it is.
const CHILDREN_COLLAPSED_NOTICE: &str = "_Live state at composition, one line per child; an open child's detail is at its URI. This seed exceeded its budget, so it carries addresses where it would otherwise carry detail._";

const NO_CHILDREN_NOTICE: &str = "_This thread has no children._\n\n";
const CENSUS_TERMINAL_CHILDREN: usize = 30;
const CENSUS_TERMINAL_BRANCHES: usize = 30;

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
    pub minted_eras: Vec<crate::threads::compaction::EraMint>,
    pub arc_bytes: i64,
    pub eras_through_seq: Option<i64>,
}

/// Compose the seed for `job`'s next thread session.
///
/// Fails rather than returning a partial seed: the caller preserves the existing
/// session on any error, exactly as the ordinary reseed path does.
#[cfg(test)]
pub(crate) async fn compose_thread_seed(
    db: &LocalDb,
    job_id: &str,
    now: i64,
) -> Result<ThreadSeed, String> {
    compose_thread_seed_with_policy(db, job_id, now, SeedPolicy::default()).await
}

pub(crate) async fn compose_thread_seed_with_policy(
    db: &LocalDb,
    job_id: &str,
    now: i64,
    policy: SeedPolicy,
) -> Result<ThreadSeed, String> {
    // No connection is held across the transcript load or the composition
    // below: either would keep a transaction open for as long as a long
    // session takes to read and render, which blocks the checkpoint
    // (tenet 1260).
    let events = load_job_events_ordered(db, job_id).await.map_err(|error| {
        format!("Cannot compose a thread seed: its transcript could not be read: {error}")
    })?;
    if events.is_empty() {
        return Err(
            "Cannot compose a thread seed: this job has no resumable transcript.".to_string(),
        );
    }

    let (coordinates, source_session_id, children, branches, store) = {
        let conn = connect_for_read(db).await?;
        let coordinates = node_coordinates(&conn, job_id).await.ok_or_else(|| {
            "Cannot compose a thread seed: the node has no addressable coordinates.".to_string()
        })?;
        let source_session_id = current_session_id(&conn, job_id).await.ok_or_else(|| {
            "Cannot compose a thread seed: the job has no current session.".to_string()
        })?;
        let children = load_children(&conn, job_id)
            .await
            .map_err(|error| format!("Failed to resolve this thread's children: {error}"))?;
        let branches = load_branches(&conn, job_id)
            .await
            .map_err(|error| format!("Failed to resolve this thread's branches: {error}"))?;
        let store = thread_store_census(&conn, job_id, now).await;
        (coordinates, source_session_id, children, branches, store)
    };
    let chat_uri = format!("{}/chat", coordinates.base_uri);

    let marks = compaction::unconsumed_marks(db, job_id, &source_session_id)
        .await
        .map_err(|error| format!("Failed to load thread compaction marks: {error}"))?;
    let mut prior_entries = compaction::applied_entries(db, job_id, &source_session_id)
        .await
        .map_err(|error| format!("Failed to load the prior thread fold boundaries: {error}"))?;
    let existing_eras = crate::threads::eras::list_eras(db, job_id, i64::MAX, 0)
        .await
        .map_err(|error| format!("Failed to load thread eras: {error}"))?;
    let era_end = existing_eras.iter().map(|era| era.end_block).max();
    if let Some(end) = era_end {
        prior_entries.push(TocEntry {
            source: EntrySource::Interstitial,
            overview: String::new(),
            content_uri: "era:covered".to_string(),
            child_issue_id: None,
            start_block: 0,
            end_block: end,
            start_turn_id: None,
            end_turn_id: None,
        });
    }
    let open_children: Vec<String> = children
        .iter()
        .filter(|child| child.is_open())
        .map(|child| child.uri.clone())
        .collect();

    let blocks = group_turn_blocks(&events);
    let plan = plan_chapters(
        &blocks,
        &marks,
        &open_children,
        &prior_entries,
        blocks.len().saturating_sub(RECENCY_FLOOR_TURNS),
    );

    let new_entries: Vec<TocEntry> = plan
        .chapters
        .iter()
        .map(|chapter| build_entry(chapter, &blocks, &marks, &chat_uri))
        .collect();

    let mut entries: Vec<TocEntry> = prior_entries
        .into_iter()
        .filter(|entry| entry.content_uri != "era:covered")
        .collect();
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
        coordinate: coordinates.digest_coordinate(),
        status: &coordinates.status,
    };

    let arc = render_arc(db, job_id, now).await;
    let branches_census = render_branches(&branches, now);
    let census = render_children(&children, now);

    let mut era_text = String::from("## Eras\n\n");
    let inline_count = policy.inline_eras.min(existing_eras.len());
    if existing_eras.len() > inline_count {
        era_text.push_str(&format!(
            "_{} older eras → `cairn:~/eras`._\n\n",
            existing_eras.len() - inline_count
        ));
    }
    for era in existing_eras.iter().take(inline_count).rev() {
        let range_uri = format!(
            "{chat_uri}?offset={}&limit={}",
            era.start_block,
            era.end_block - era.start_block + 1
        );
        era_text.push_str(&format!(
            "- **Era {}.** {} → {}\n",
            era.seq,
            era.summary.trim(),
            range_uri
        ));
    }
    if existing_eras.is_empty() {
        era_text.push_str(NO_HISTORY_NOTICE);
        era_text.push('\n');
    }
    era_text.push('\n');

    let framing_tokens = seed_tokens(&branches_census)
        + seed_tokens(&census)
        + seed_tokens(&store)
        + seed_tokens(&era_text);
    let verbatim_budget = policy.budget_tokens.saturating_sub(framing_tokens);
    let floor_start = blocks.len().saturating_sub(RECENCY_FLOOR_TURNS);
    let live_start = era_end.map_or(0, |end| (end + 1).max(0) as usize);
    let live_tokens: usize = (live_start..blocks.len())
        .map(|index| seed_tokens(&block_text(&blocks[index])))
        .sum();

    let mut minted_eras = Vec::new();
    let mut covered_through = era_end;
    if live_tokens > verbatim_budget && live_start < floor_start {
        let target = ERA_MIN_SOURCE_TOKENS.max(live_tokens.saturating_sub(verbatim_budget / 2));
        let mut taken_tokens = 0usize;
        let mut end = live_start;
        while end < floor_start && taken_tokens < target {
            taken_tokens += seed_tokens(&block_text(&blocks[end]));
            end += 1;
        }
        if end > live_start {
            let end_index = end - 1;
            let span_text = (live_start..=end_index)
                .map(|index| block_text(&blocks[index]))
                .collect::<Vec<_>>()
                .join("\n");
            let era_children: Vec<ChildMark> = marks
                .iter()
                .filter(|mark| mentions_uri(&span_text, &mark.child_issue_uri))
                .cloned()
                .collect();
            let child_text = if era_children.is_empty() {
                "no child resolutions".to_string()
            } else {
                era_children
                    .iter()
                    .map(|child| format!("{} ({})", child.child_title, child.child_issue_uri))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let start_at = blocks[live_start]
                .events
                .first()
                .map_or(now, |event| event.created_at);
            let end_at = blocks[end_index]
                .events
                .last()
                .map_or(now, |event| event.created_at);
            let summary = format!(
                "Turns {}–{} ({} to {}) covered {}; the source remains addressable in this era's chat range.",
                live_start + 1,
                end_index + 1,
                start_at,
                end_at,
                child_text
            );
            minted_eras.push(crate::threads::compaction::EraMint {
                start_block: live_start as i64,
                end_block: end_index as i64,
                started_at: start_at,
                ended_at: end_at,
                summary,
                source_tokens: taken_tokens as i64,
                children: era_children,
            });
            covered_through = Some(end_index as i64);
        }
    }

    // Selection priority differs from render order: preserve immediate recency,
    // then unresolved-child turns within half the window, then fill newest-first.
    let mut selected: Vec<usize> = (floor_start..blocks.len()).collect();
    let pinned_cap = verbatim_budget * UNRESOLVED_CHILD_SHARE / 100;
    let mut pinned_tokens = 0usize;
    for index in (0..floor_start).rev() {
        if !open_children
            .iter()
            .any(|uri| mentions_uri(&block_text(&blocks[index]), uri))
        {
            continue;
        }
        let weight = seed_tokens(&block_text(&blocks[index]));
        if pinned_tokens + weight <= pinned_cap {
            selected.push(index);
            pinned_tokens += weight;
        }
    }
    for index in (0..floor_start).rev() {
        if selected.contains(&index) || covered_through.is_some_and(|end| index as i64 <= end) {
            continue;
        }
        let mut trial = selected.clone();
        trial.push(index);
        trial.sort_unstable();
        if recent_section_tokens(&blocks, &trial, &chat_uri, &meta) <= verbatim_budget {
            selected = trial;
        }
    }
    selected.sort_unstable();
    while recent_section_tokens(&blocks, &selected, &chat_uri, &meta) > verbatim_budget {
        let Some(position) = selected.iter().position(|index| *index < floor_start) else {
            break;
        };
        selected.remove(position);
    }
    if let Some(end) = covered_through {
        entries.retain(|entry| entry.end_block > end);
    }

    let mut body = String::new();
    body.push_str(&branches_census);
    body.push_str(&census);
    body.push_str(&store);
    body.push_str(&era_text);
    body.push_str("## Recent\n\n");
    body.push_str(&render_gap_markers(&selected, blocks.len(), &chat_uri));
    body.push_str(&render_blocks(&blocks, &selected, &chat_uri, &meta));

    // Where the configured bound becomes true rather than merely intended.
    // Everything above budgets the parts it is free to choose, around two it is
    // not: the whole open-child census and the recency floor. Measuring the
    // composed body is the only check that sees what those two actually cost.
    let mut recency_start = floor_start;
    if seed_tokens(&body) > policy.budget_tokens {
        let degraded = degrade_to_budget(
            policy.budget_tokens,
            &children,
            &branches_census,
            &store,
            existing_eras.len(),
            &coordinates.base_uri,
            &blocks,
            &chat_uri,
            &meta,
        );
        body = degraded.body;
        selected = degraded.selected;
        recency_start = selected.first().copied().unwrap_or(blocks.len());
    }

    let dropped_indices: Vec<usize> = (0..blocks.len())
        .filter(|index| !selected.contains(index))
        .collect();
    let dropped = render_blocks(&blocks, &dropped_indices, &chat_uri, &meta);

    let mut content = String::new();
    content.push_str("## Arc\n\n");
    content.push_str(arc.trim());
    content.push_str("\n\n");
    content.push_str(&body);

    let consumed_child_issue_ids = minted_eras
        .iter()
        .flat_map(|era| {
            era.children
                .iter()
                .map(|child| child.child_issue_id.clone())
        })
        .collect::<Vec<_>>();
    let eras_through_seq = existing_eras.first().map(|era| era.seq as i64);

    Ok(ThreadSeed {
        content,
        source_bytes: dropped.len() as i64,
        candidate_bytes: era_text.len() as i64,
        new_entries: new_entries.len(),
        entries,
        compacted_through_block: covered_through,
        recency_start_block: recency_start as i64,
        consumed_child_issue_ids,
        source_session_id,
        minted_eras,
        arc_bytes: arc.len() as i64,
        eras_through_seq,
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
}

/// Decide, per turn below `recency_start`, whether it is dropped into a chapter
/// or kept verbatim. Turns from `recency_start` on are never chaptered.
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
    recency_start: usize,
) -> ChapterPlan {
    let already_compacted = |index: usize| {
        prior_entries
            .iter()
            .any(|entry| (entry.start_block..=entry.end_block).contains(&(index as i64)))
    };

    let mut chapters: Vec<Chapter> = Vec::new();
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
            continue;
        }

        let text = block_text(block);
        if open_children.iter().any(|uri| mentions_uri(&text, uri)) {
            // Never drop a turn that also touches work still in flight.
            if let Some(chapter) = open.take() {
                chapters.push(chapter);
            }
            continue;
        }

        let kind = match marks
            .iter()
            .position(|mark| mentions_uri(&text, &mark.child_issue_uri))
        {
            Some(mark) => ChapterKind::Child(mark),
            None => ChapterKind::Interstitial,
        };
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

    ChapterPlan { chapters }
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

/// Render a set of turns through the ordinary transcript renderer at reseed
/// fidelity, so kept turns read exactly as they would have without compaction.
///
/// Turns are labelled by their index in the job's whole block sequence, not by
/// their position in the rendered subset, so "turns 1–4" in the table of
/// contents and "Turn 5" in the verbatim section describe one numbering. A
/// subset-relative label would tell a thread its oldest kept turn is turn 1.
fn render_gap_markers(selected: &[usize], total: usize, chat_uri: &str) -> String {
    // A set, because a node's history can run to thousands of turns and this is
    // measured once per candidate selection.
    let selected: HashSet<usize> = selected.iter().copied().collect();
    let mut out = String::new();
    let mut start = None;
    for index in 0..total {
        if selected.contains(&index) {
            if let Some(gap_start) = start.take() {
                out.push_str(&omitted_range_marker(gap_start, index, chat_uri));
            }
        } else if start.is_none() {
            start = Some(index);
        }
    }
    if let Some(gap_start) = start {
        out.push_str(&omitted_range_marker(gap_start, total, chat_uri));
    }
    out
}

/// The one line that stands for the turns `start..end` (block indices, end
/// exclusive) and says where to read them in full.
fn omitted_range_marker(start: usize, end: usize, chat_uri: &str) -> String {
    format!(
        "_Turns {}–{end} omitted → `{chat_uri}?offset={start}&limit={}`._\n\n",
        start + 1,
        end - start
    )
}

fn recent_section_tokens(
    blocks: &[TurnBlock<'_>],
    selected: &[usize],
    chat_uri: &str,
    meta: &DigestMeta<'_>,
) -> usize {
    let mut section = String::from("## Recent\n\n");
    section.push_str(&render_gap_markers(selected, blocks.len(), chat_uri));
    section.push_str(&render_blocks(blocks, selected, chat_uri, meta));
    seed_tokens(&section)
}

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
        crate::clock::host(),
        &DigestOptions {
            latest: false,
            turn_offset: None,
            turn_limit: None,
            unabridged: true,
            inline_diffs: true,
        },
    )
}

// ── The budget's last boundary ──────────────────────────────────────────────

/// A non-arc body recomposed to fit, and the turns that survived into it.
struct DegradedBody {
    body: String,
    selected: Vec<usize>,
}

/// Recompose the non-arc body when the ordinary policy does not fit `budget`.
///
/// Trimming that policy would not help: what fails to fit is its shape, not its
/// margin. A thread with sixty open children pays their detail blocks whatever
/// the budget says, and one turn carrying a large file read outweighs the budget
/// by itself. So this composes a smaller shape under an inverted priority. The
/// newest turn is the center, because it is the only part of a seed that no
/// other address carries as cheaply; every child collapses to the one line that
/// addresses it; and nothing is dropped without leaving the URI that reaches it.
#[allow(clippy::too_many_arguments)]
fn degrade_to_budget(
    budget: usize,
    children: &[ChildStatus],
    branches: &str,
    store: &str,
    era_count: usize,
    thread_uri: &str,
    blocks: &[TurnBlock<'_>],
    chat_uri: &str,
    meta: &DigestMeta<'_>,
) -> DegradedBody {
    let eras = collapsed_eras(era_count);
    // What the newest turn actually costs, capped at half the budget. The census
    // may take everything that reserve does not: children are usually the bulk
    // of a seed, and holding a fixed half open for a two-line turn would drop
    // children to protect space nothing uses. The cap is what keeps the reverse
    // true — a thread with sixty children cannot crowd out its own present.
    let reserve = blocks
        .len()
        .checked_sub(1)
        .map(|newest| recent_section_tokens(blocks, &[newest], chat_uri, meta).min(budget / 2))
        .unwrap_or_default();
    let census = collapsed_census(
        children,
        thread_uri,
        budget.saturating_sub(
            reserve + seed_tokens(branches) + seed_tokens(store) + seed_tokens(&eras),
        ),
    );
    let framing =
        seed_tokens(branches) + seed_tokens(&census) + seed_tokens(store) + seed_tokens(&eras);
    let recent = bounded_recent(
        blocks,
        budget.saturating_sub(framing),
        chat_uri,
        meta,
        RECENCY_FLOOR_TURNS,
    );

    let body = format!("{branches}{census}{store}{eras}{}", recent.section);
    if seed_tokens(&body) <= budget {
        return DegradedBody {
            body,
            selected: recent.selected,
        };
    }

    // A budget smaller than the framing that introduces it. The census and the
    // eras each have an address of their own and the turn does not, so the turn
    // takes what is left and the cut makes that literally true.
    let recent = bounded_recent(blocks, budget, chat_uri, meta, RECENCY_FLOOR_TURNS);
    let mut body = recent.section;
    if seed_tokens(&body) > budget {
        body.truncate(seed_cut(&body, budget));
    }
    DegradedBody {
        body,
        selected: recent.selected,
    }
}

/// The eras as a single address. An era summary is a sentence and three of them
/// render inline; under pressure the collection URI carries all of it.
async fn thread_store_census(conn: &cairn_db::turso::Connection, job_id: &str, now: i64) -> String {
    let Ok(mut rows) = conn
        .query("SELECT thread_id FROM jobs WHERE id = ?1", params![job_id])
        .await
    else {
        return String::new();
    };
    let Ok(Some(row)) = rows.next().await else {
        return String::new();
    };
    let Ok(thread_id) = row.get::<String>(0) else {
        return String::new();
    };
    let Some(census) = crate::threads::db_store::census(&thread_id).await else {
        return String::new();
    };
    render_thread_store_census(&census, now)
}

fn render_thread_store_census(
    census: &crate::threads::db_store::ThreadDbCensus,
    now: i64,
) -> String {
    const MAX_TABLES: usize = 8;
    const MAX_NAME_CHARS: usize = 40;
    let mut tables = census
        .tables
        .iter()
        .take(MAX_TABLES)
        .map(|(name, rows)| {
            let flattened = name
                .chars()
                .map(|ch| {
                    if ch.is_control() || ch.is_whitespace() {
                        ' '
                    } else {
                        ch
                    }
                })
                .collect::<String>()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            let mut bounded = flattened.chars().take(MAX_NAME_CHARS).collect::<String>();
            if flattened.chars().count() > MAX_NAME_CHARS {
                bounded.push('…');
            }
            if bounded.is_empty() {
                bounded.push_str("(unnamed)");
            }
            format!("{bounded} {} rows", format_count(*rows))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let omitted = census.tables.len().saturating_sub(MAX_TABLES);
    if omitted > 0 {
        tables.push_str(&format!(", … {omitted} more tables → `cairn:~/db`"));
    }
    let tables = if tables.is_empty() {
        "no tables".to_string()
    } else {
        tables
    };
    let mut details = vec![tables, format_bytes(census.bytes)];
    if let Some(last_write) = census.last_write {
        let elapsed = now.saturating_sub(last_write);
        details.push(if elapsed < 60 {
            "last write just now".to_string()
        } else {
            format!("last write {} ago", crate::clock::format_elapsed(elapsed))
        });
    }
    format!("- **Store.** `cairn:~/db` — {}.\n\n", details.join(" · "))
}

fn format_count(value: i64) -> String {
    let digits = value.to_string();
    let (sign, digits) = digits
        .strip_prefix('-')
        .map_or(("", digits.as_str()), |rest| ("-", rest));
    let mut out = String::with_capacity(digits.len() + digits.len() / 3 + sign.len());
    out.push_str(sign);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["bytes", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn collapsed_eras(count: usize) -> String {
    let mut out = String::from("## Eras\n\n");
    match count {
        0 => out.push_str(NO_HISTORY_NOTICE),
        1 => out.push_str("_1 era → `cairn:~/eras`._"),
        count => out.push_str(&format!("_{count} eras → `cairn:~/eras`._")),
    }
    out.push_str("\n\n");
    out
}

/// Every child on one line, fitted to `budget`, with an address for the rest.
///
/// Open children lose their detail blocks here, and terminal ones lose the
/// separate cap they carry in an ordinary census — the fit is the only bound
/// this needs. Dropping detail before dropping children is the right order:
/// detail is live state one read away at the URI still on the line, while a
/// child that vanishes from the census is a child the thread does not know it
/// has.
fn collapsed_census(children: &[ChildStatus], thread_uri: &str, budget: usize) -> String {
    let (open, mut terminal): (Vec<_>, Vec<_>) = children.iter().partition(|child| child.is_open());
    terminal.sort_by_key(|child| std::cmp::Reverse(child.last_activity_at.unwrap_or(0)));
    let lines: Vec<String> = open
        .into_iter()
        .chain(terminal)
        .map(ChildStatus::census_line)
        .collect();

    // The longest prefix that fits, found by binary search over the measured
    // section. Only a measured candidate is ever accepted, so the bound holds
    // whatever the search converges on, and the marker standing for the rest is
    // inside every measurement rather than added to a section already sized.
    let (mut low, mut high) = (0usize, lines.len());
    let mut best = 0usize;
    while low <= high {
        let mid = low + (high - low) / 2;
        if seed_tokens(&render_collapsed_census(&lines, mid, thread_uri)) <= budget {
            best = mid;
            low = mid + 1;
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
    render_collapsed_census(&lines, best, thread_uri)
}

fn render_collapsed_census(lines: &[String], take: usize, thread_uri: &str) -> String {
    let mut out = String::from("## Children\n\n");
    out.push_str(CHILDREN_COLLAPSED_NOTICE);
    out.push_str("\n\n");
    if lines.is_empty() {
        out.push_str(NO_CHILDREN_NOTICE);
        return out;
    }
    for line in lines.iter().take(take) {
        out.push_str(line);
    }
    if take < lines.len() {
        out.push_str(&format!(
            "- _… and {} more children → `{thread_uri}`._\n",
            lines.len() - take
        ));
    }
    out.push('\n');
    out
}

/// The `## Recent` section and the turns it carries.
struct BoundedRecent {
    section: String,
    selected: Vec<usize>,
}

/// The newest turns that fit `budget`, at most `max_turns` and never fewer than
/// one.
///
/// A degrading thread seed passes its recency floor as the ceiling; a node seed
/// passes no ceiling, because its verbatim window is all the history it carries.
/// Either way this takes newest-first and stops at the first turn that would not
/// fit — and when even the newest turn is bigger than the budget, keeps its head
/// and says where the rest of it is. An over-budget seed and an empty one are
/// both worse than a turn an agent can see was cut.
fn bounded_recent(
    blocks: &[TurnBlock<'_>],
    budget: usize,
    chat_uri: &str,
    meta: &DigestMeta<'_>,
    max_turns: usize,
) -> BoundedRecent {
    let Some(newest) = blocks.len().checked_sub(1) else {
        return BoundedRecent {
            section: String::from("## Recent\n\n"),
            selected: Vec::new(),
        };
    };

    let mut selected = vec![newest];
    for index in (blocks.len().saturating_sub(max_turns)..newest).rev() {
        let mut trial = selected.clone();
        trial.push(index);
        trial.sort_unstable();
        if recent_section_tokens(blocks, &trial, chat_uri, meta) > budget {
            break;
        }
        selected = trial;
    }

    let mut section = String::from("## Recent\n\n");
    section.push_str(&render_gap_markers(&selected, blocks.len(), chat_uri));
    section.push_str(&render_blocks(blocks, &selected, chat_uri, meta));
    if seed_tokens(&section) <= budget {
        return BoundedRecent { section, selected };
    }

    BoundedRecent {
        section: cut_newest_turn(blocks, newest, budget, chat_uri, meta),
        selected: vec![newest],
    }
}

/// The head of one turn, under `budget`, followed by the address of the whole.
fn cut_newest_turn(
    blocks: &[TurnBlock<'_>],
    index: usize,
    budget: usize,
    chat_uri: &str,
    meta: &DigestMeta<'_>,
) -> String {
    let mut head = String::from("## Recent\n\n");
    head.push_str(&render_gap_markers(&[index], blocks.len(), chat_uri));
    let marker = format!(
        "\n_Turn {} continues → `{chat_uri}?offset={index}&limit=1`._\n\n",
        index + 1
    );
    let rendered = render_blocks(blocks, &[index], chat_uri, meta);

    // A cut prefix does not cost what the same tokens cost mid-document, so the
    // allowance is corrected against the assembled section rather than trusted.
    let mut allowance = budget.saturating_sub(seed_tokens(&head) + seed_tokens(&marker));
    loop {
        let section = format!(
            "{head}{}{marker}",
            &rendered[..seed_cut(&rendered, allowance)]
        );
        let tokens = seed_tokens(&section);
        if tokens <= budget || allowance == 0 {
            return section;
        }
        allowance = allowance.saturating_sub((tokens - budget).max(1));
    }
}

async fn render_arc(db: &LocalDb, job_id: &str, now: i64) -> String {
    match crate::artifacts::queries::get_named(db, job_id, ARC_ARTIFACT_NAME).await {
        Ok(Some(artifact)) => {
            match crate::threads::arc_store::render(db, job_id, &artifact.data, now).await {
                Ok(rendered) => rendered,
                Err(error) => {
                    log::warn!("thread arc projection failed for job {job_id}: {error}");
                    render_arc_data(&artifact.data)
                }
            }
        }
        // A thread whose agent has not written an arc still gets a valid seed;
        // it is told, in the seed, that the one irreplaceable section is empty.
        Ok(None) => ARC_MISSING_NOTICE.to_string(),
        Err(error) => {
            log::warn!("thread arc could not be read for job {job_id}: {error}");
            ARC_MISSING_NOTICE.to_string()
        }
    }
}

/// Render the authored prose of an arc as stable Markdown, copying every
/// authored string exactly. Anything this does not recognize is passed through
/// rather than dropped: the compactor is not allowed to decide that authored
/// intent was unimportant. The collections an older arc embedded (`rulings`,
/// `open_questions`) are not prose and are not rendered here; their homes are
/// tenets and the thread store.
pub(crate) fn render_arc_data(data: &serde_json::Value) -> String {
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

    if let Some(working) = data
        .get("working")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        out.push_str("\n### Working (volatile)\n\n");
        out.push_str(working);
        out.push('\n');
    }

    if out.trim().is_empty() {
        // Not the provenance-first shape. Copy whatever the thread did author.
        let mut rest = data.clone();
        crate::threads::arc_store::retain_prose_only(&mut rest);
        return data
            .get("content")
            .or_else(|| data.get("body"))
            .and_then(|value| value.as_str())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| match &rest {
                serde_json::Value::Null => String::new(),
                serde_json::Value::Object(object) if object.is_empty() => String::new(),
                other => compact_json(other),
            });
    }
    out
}

// ── Tenets in the arc ───────────────────────────────────────────────────────

/// What the full-form tenets section may weigh. Tenets past it are still
/// listed, one line each, in the index.
pub(crate) const TENET_FULL_BUDGET_TOKENS: usize = 3_000;

/// How long an index line's first sentence may run.
const TENET_INDEX_SENTENCE_CHARS: usize = 160;

/// The cosine similarity another thread's tenet needs to some piece of this
/// thread's work to be shown as related.
const RELATED_MIN_SIMILARITY: f32 = 0.5;

/// What recent evidence adds to a tenet's relevance score.
const RECENT_EVIDENCE_BOOST: f32 = 0.05;

/// What a selector ranks from.
pub(crate) struct TenetSelection<'a> {
    pub thread_id: &'a str,
    pub thread_name: &'a str,
    pub project_id: &'a str,
    pub project_key: &'a str,
    /// Every tenet this thread proposed, oldest first.
    pub own: &'a [TenetView],
    /// The private database holding resource vectors. Composition only reads
    /// stored vectors; it never reaches the embedding gateway.
    pub vectors: &'a LocalDb,
    pub now: i64,
}

/// One tenet a selector chose to show in full, in the order it should appear.
#[derive(Debug, Clone)]
pub(crate) struct RankedTenet {
    pub tenet: TenetView,
    /// Proposed by this thread, rather than related from another.
    pub own: bool,
}

/// Which tenets an arc shows in full, best first. The composer owns the
/// budget, the index, and the awaiting-verdict list; a selector only orders.
#[async_trait::async_trait]
pub(crate) trait TenetSelector: Send + Sync {
    async fn rank(&self, db: &LocalDb, input: &TenetSelection<'_>) -> Vec<RankedTenet>;
}

/// The thread's own approved tenets, most recently answered first.
pub(crate) struct RecencySelector;

#[async_trait::async_trait]
impl TenetSelector for RecencySelector {
    async fn rank(&self, _db: &LocalDb, input: &TenetSelection<'_>) -> Vec<RankedTenet> {
        recency_rank(input.own)
    }
}

fn recency_rank(own: &[TenetView]) -> Vec<RankedTenet> {
    let mut approved: Vec<_> = own
        .iter()
        .filter(|view| view.row.status == TenetStatus::Approved)
        .cloned()
        .collect();
    approved.sort_by_key(|view| {
        (
            std::cmp::Reverse(view.row.answered_at.unwrap_or(0)),
            std::cmp::Reverse(view.row.number),
        )
    });
    approved
        .into_iter()
        .map(|tenet| RankedTenet { tenet, own: true })
        .collect()
}

/// Tenets ranked by how near they are to what the thread is working on: its
/// arc and its open child issues. The thread's own approved tenets come first,
/// best first, then its own tenets that have no vector yet in recency order;
/// other threads' approved tenets follow when they are near enough. With no
/// vector for the arc or any open child, this is [`RecencySelector`].
pub(crate) struct RelevanceSelector;

#[async_trait::async_trait]
impl TenetSelector for RelevanceSelector {
    async fn rank(&self, db: &LocalDb, input: &TenetSelection<'_>) -> Vec<RankedTenet> {
        match relevance_rank(db, input).await {
            Ok(Some(ranked)) => ranked,
            Ok(None) => RecencySelector.rank(db, input).await,
            Err(error) => {
                log::warn!("tenet relevance ranking failed; ranking by recency: {error}");
                RecencySelector.rank(db, input).await
            }
        }
    }
}

async fn relevance_rank(
    db: &LocalDb,
    input: &TenetSelection<'_>,
) -> Result<Option<Vec<RankedTenet>>, String> {
    use crate::tenets::evidence::vectors::{project_tenet_vectors, resource_vector};

    let mut work_uris = vec![format!(
        "{}/arc",
        cairn_common::uri::build_thread_uri(input.project_key, input.thread_name)
    )];
    let open_children = db
        .query_all(
            "SELECT number FROM issues
             WHERE parent_thread_id = ?1 AND status NOT IN ('merged','closed')",
            params![input.thread_id],
            |row| row.i64(0),
        )
        .await
        .map_err(|error| format!("failed to read open children: {error}"))?;
    work_uris.extend(
        open_children
            .into_iter()
            .map(|number| cairn_common::uri::build_issue_uri(input.project_key, number as i32)),
    );
    let mut work: Vec<(crate::embeddings::VectorKey, Vec<f32>)> = Vec::new();
    for uri in &work_uris {
        if let Some(stored) = resource_vector(input.vectors, uri)
            .await
            .map_err(|error| format!("failed to read the vector of {uri}: {error}"))?
        {
            work.push(stored);
        }
    }
    if work.is_empty() {
        return Ok(None);
    }

    // A piece of work is compared only with tenets embedded in its own space.
    let mut tenet_vectors: Vec<(crate::embeddings::VectorKey, HashMap<String, Vec<f32>>)> =
        Vec::new();
    for (key, _) in &work {
        if tenet_vectors.iter().any(|(known, _)| known == key) {
            continue;
        }
        let vectors = project_tenet_vectors(input.vectors, input.project_key, key)
            .await
            .map_err(|error| format!("failed to read tenet vectors: {error}"))?;
        tenet_vectors.push((key.clone(), vectors));
    }
    let similarity = |view: &TenetView| -> Option<f32> {
        work.iter()
            .filter_map(|(key, other)| {
                let (_, space) = tenet_vectors.iter().find(|(known, _)| known == key)?;
                let vector = space.get(&view.uri)?;
                (vector.len() == other.len())
                    .then(|| crate::embeddings::vector::cosine_similarity(vector, other))
            })
            .reduce(f32::max)
    };
    let recent_since = input.now - crate::tenets::evidence::RECENT_EVIDENCE_SECS;
    let score = |view: &TenetView, raw: f32| -> f32 {
        let recent = view
            .evidence
            .iter()
            .any(|evidence| evidence.evaluated_at >= recent_since);
        raw + if recent { RECENT_EVIDENCE_BOOST } else { 0.0 }
    };
    let best_first = |a: &(f32, TenetView), b: &(f32, TenetView)| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.row.number.cmp(&b.1.row.number))
    };

    let mut scored_own: Vec<(f32, TenetView)> = Vec::new();
    let mut unscored_own: Vec<TenetView> = Vec::new();
    for view in input
        .own
        .iter()
        .filter(|view| view.row.status == TenetStatus::Approved)
    {
        match similarity(view) {
            Some(raw) => scored_own.push((score(view, raw), view.clone())),
            None => unscored_own.push(view.clone()),
        }
    }
    scored_own.sort_by(best_first);

    let approved = crate::tenets::list(
        db,
        input.project_id,
        &crate::tenets::TenetFilter {
            statuses: vec![TenetStatus::Approved],
            ..Default::default()
        },
    )
    .await?;
    let mut related: Vec<(f32, TenetView)> = approved
        .into_iter()
        .filter(|view| view.row.proposing_thread_id != input.thread_id)
        .filter_map(|view| {
            let raw = similarity(&view)?;
            (raw >= RELATED_MIN_SIMILARITY).then(|| (score(&view, raw), view))
        })
        .collect();
    related.sort_by(best_first);

    let mut ranked: Vec<RankedTenet> = scored_own
        .into_iter()
        .map(|(_, tenet)| RankedTenet { tenet, own: true })
        .collect();
    ranked.extend(recency_rank(&unscored_own));
    ranked.extend(
        related
            .into_iter()
            .map(|(_, tenet)| RankedTenet { tenet, own: false }),
    );
    Ok(Some(ranked))
}

pub(crate) fn default_tenet_selector() -> &'static dyn TenetSelector {
    &RelevanceSelector
}

/// Render an arc: the authored prose, the thread's tenets awaiting a verdict,
/// its selected tenets in full under [`TENET_FULL_BUDGET_TOKENS`], a one-line
/// index of the rest, and its open questions.
///
/// A withdrawn tenet never renders. A declined or superseded one is never in
/// the full slice, so it always lands in the index.
pub(crate) fn render_arc_projection(projection: &ArcProjection, ranked: &[RankedTenet]) -> String {
    let mut out = render_arc_data(&projection.prose);

    let visible: Vec<&TenetView> = projection
        .tenets
        .iter()
        .filter(|view| view.row.status != TenetStatus::Withdrawn)
        .collect();
    let mut awaiting: Vec<&TenetView> = visible
        .iter()
        .copied()
        .filter(|view| view.row.status == TenetStatus::Proposed)
        .collect();
    awaiting.sort_by_key(|view| view.row.number);
    if !awaiting.is_empty() {
        out.push_str("\n### Awaiting verdict\n\n");
        for view in awaiting {
            out.push_str(&render_awaiting_tenet(view));
        }
    }

    let mut full = String::new();
    let mut shown: HashSet<i64> = HashSet::new();
    for ranked in ranked.iter().filter(|ranked| ranked.own) {
        let view = &ranked.tenet;
        if view.row.status != TenetStatus::Approved || shown.contains(&view.row.number) {
            continue;
        }
        let candidate = format!("{full}{}", render_full_tenet(view));
        if seed_tokens(&candidate) <= TENET_FULL_BUDGET_TOKENS {
            full = candidate;
            shown.insert(view.row.number);
        }
    }
    // Other threads' tenets take whatever budget the thread's own leave, and
    // never reach the index, which lists only this thread's tenets.
    let mut related = String::new();
    for ranked in ranked.iter().filter(|ranked| !ranked.own) {
        let view = &ranked.tenet;
        if view.row.status != TenetStatus::Approved {
            continue;
        }
        let candidate = format!("{related}{}", render_related_tenet(view));
        if seed_tokens(&format!("{full}{candidate}")) <= TENET_FULL_BUDGET_TOKENS {
            related = candidate;
        }
    }
    if !full.is_empty() {
        out.push_str("\n### Tenets\n\n");
        out.push_str(&full);
    }
    if !related.is_empty() {
        out.push_str("\n### Related tenets from other threads\n\n");
        out.push_str(&related);
    }

    if !visible.is_empty() {
        let mut index: Vec<&TenetView> = visible
            .iter()
            .copied()
            .filter(|view| {
                view.row.status != TenetStatus::Proposed && !shown.contains(&view.row.number)
            })
            .collect();
        index.sort_by_key(|view| view.row.number);
        if !index.is_empty() {
            out.push_str("\n### Tenet index\n\n");
            for view in index {
                out.push_str(&render_index_tenet(view));
            }
        }

        let tenets = cairn_common::uri::build_project_tenets_uri(&projection.project_key);
        out.push_str(&format!(
            "\nFull text: {tenets}/<n> · all of this thread's: {tenets}?thread={}\n",
            projection.thread_name
        ));
    }

    if !projection.open_questions.is_empty() {
        out.push_str("\n### Open questions\n\n");
        for question in &projection.open_questions {
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
    out
}

/// A UTC calendar date, as tenets print beside a verdict.
fn tenet_date(at: i64) -> String {
    chrono::DateTime::from_timestamp(at, 0)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| at.to_string())
}

fn render_awaiting_tenet(view: &TenetView) -> String {
    format!(
        "- **#{}** (proposed {}) — {}\n",
        view.row.number,
        tenet_date(view.row.created_at),
        view.row.statement
    )
}

fn render_full_tenet(view: &TenetView) -> String {
    render_tenet_entry(view, None)
}

/// Another thread's tenet, named by the thread that proposed it.
fn render_related_tenet(view: &TenetView) -> String {
    render_tenet_entry(view, Some(&view.proposing_thread))
}

fn render_tenet_entry(view: &TenetView, from_thread: Option<&str>) -> String {
    let row = &view.row;
    let from = from_thread
        .map(|thread| format!("{thread}, "))
        .unwrap_or_default();
    let mut entry = format!(
        "- **#{}** ({from}approved by {}, {}) — {}\n",
        row.number,
        view.answerer_label.as_deref().unwrap_or("the operator"),
        tenet_date(row.answered_at.unwrap_or(row.updated_at)),
        row.statement
    );
    if let Some(comment) = &row.comment {
        entry.push_str(&format!("  comment: {comment}\n"));
    }
    if let Some(previous) = row.supersedes {
        entry.push_str(&format!("  supersedes #{previous}\n"));
    }
    if row.origin == TenetOrigin::MigratedRuling {
        if let Some(slug) = &row.slug {
            entry.push_str(&format!("  slug: `{slug}`\n"));
        }
    }
    // The newest few references; the tenet's own record carries them all.
    if !view.evidence.is_empty() {
        let references = view
            .evidence
            .iter()
            .take(3)
            .map(|evidence| {
                format!(
                    "{} {} ({:.2}, {})",
                    evidence.sign,
                    evidence.source_uri,
                    evidence.confidence,
                    crate::tenets::evidence::utc_date(evidence.evaluated_at, "%m-%d")
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        entry.push_str(&format!("  evidence: {references}\n"));
    }
    entry
}

fn render_index_tenet(view: &TenetView) -> String {
    let row = &view.row;
    let mut line = format!(
        "- #{} · {} · {}",
        row.number,
        row.status.as_str(),
        tenet_first_sentence(&row.statement, TENET_INDEX_SENTENCE_CHARS)
    );
    if let Some(slug) = &row.slug {
        line.push_str(&format!(" [{slug}]"));
    }
    line.push('\n');
    line
}

/// The first sentence of a tenet, on one line, capped at `max` characters.
/// Unlike a turn overview a tenet carries no clock stamp to strip, and its
/// opening sentence is the claim itself, so it gets more room.
fn tenet_first_sentence(text: &str, max: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let end = collapsed
        .char_indices()
        .find(|(index, ch)| {
            matches!(ch, '.' | '!' | '?')
                && collapsed[index + ch.len_utf8()..]
                    .chars()
                    .next()
                    .is_none_or(char::is_whitespace)
        })
        .map(|(index, ch)| index + ch.len_utf8())
        .unwrap_or(collapsed.len());
    let sentence = collapsed[..end].trim();
    if sentence.chars().count() <= max {
        return sentence.to_string();
    }
    let truncated: String = sentence.chars().take(max.saturating_sub(1)).collect();
    format!("{}…", truncated.trim_end())
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
    /// The owning thread, when this job is one. Threads have no issue number,
    /// so this is what the seed header names instead of an issue coordinate.
    thread_name: Option<String>,
    number: i32,
    exec_seq: i32,
    label: String,
    status: String,
}

impl NodeCoordinates {
    fn digest_coordinate(&self) -> DigestCoordinate<'_> {
        match self.thread_name.as_deref() {
            Some(name) => DigestCoordinate::Thread { name },
            None => DigestCoordinate::Issue {
                number: self.number,
                exec_seq: self.exec_seq,
            },
        }
    }
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
    // Every join to the issue side is optional: this composes the seed for a
    // THREAD, which owns no issue at all. Requiring one resolved to no row, so
    // composition failed with "the node has no addressable coordinates" for
    // exactly the sessions rolling compaction exists to serve.
    let mut rows = conn
        .query(
            "SELECT p.key, i.number, COALESCE(e.seq, 1),
                    COALESCE(j.node_name, j.uri_segment), j.status, t.name
             FROM jobs j
             LEFT JOIN issues i ON i.id = j.issue_id
             LEFT JOIN jobs parent ON j.parent_job_id = parent.id
             LEFT JOIN threads t ON t.id = COALESCE(j.thread_id, parent.thread_id)
             JOIN projects p ON p.id = COALESCE(i.project_id, j.project_id)
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
        thread_name: row.opt_text(5).ok()?,
        number: row.opt_i64(1).ok()?.unwrap_or_default() as i32,
        exec_seq: row.opt_i64(2).ok()?.unwrap_or_default() as i32,
        label: row
            .opt_text(3)
            .ok()?
            .unwrap_or_else(|| "thread".to_string()),
        status: row.text(4).ok()?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchStatus {
    number: i64,
    title: Option<String>,
    stage: String,
    moved_at: i64,
    pr: Option<(Option<i64>, String)>,
}

impl BranchStatus {
    fn is_active(&self) -> bool {
        !matches!(self.stage.as_str(), "merged" | "abandoned")
    }
}

fn render_branches(branches: &[BranchStatus], now: i64) -> String {
    let mut out = String::from("## Branches\n\n");
    if branches.is_empty() {
        out.push_str("_This thread has no branches._\n\n");
        return out;
    }
    let (active, mut terminal): (Vec<_>, Vec<_>) =
        branches.iter().partition(|branch| branch.is_active());
    terminal.sort_by_key(|branch| std::cmp::Reverse(branch.moved_at));
    for branch in active
        .into_iter()
        .chain(terminal.iter().take(CENSUS_TERMINAL_BRANCHES).copied())
    {
        let title = branch.title.as_deref().unwrap_or("Untitled");
        let age = now.saturating_sub(branch.moved_at);
        let movement = if age < 60 {
            "just now".to_string()
        } else {
            format!("{} ago", crate::clock::format_elapsed(age))
        };
        let pr = branch
            .pr
            .as_ref()
            .map(|(number, status)| match number {
                Some(number) => format!(" · PR #{number} {status}"),
                None => format!(" · PR {status}"),
            })
            .unwrap_or_default();
        out.push_str(&format!(
            "- `cairn:~/branches/{}` **{}** · {} · moved {}{}\n",
            branch.number,
            branch.stage,
            one_line(title),
            movement,
            pr
        ));
    }
    if terminal.len() > CENSUS_TERMINAL_BRANCHES {
        out.push_str(&format!(
            "- _… and {} earlier terminal branches → `cairn:~/branches`._\n",
            terminal.len() - CENSUS_TERMINAL_BRANCHES
        ));
    }
    out.push('\n');
    out
}

async fn load_branches(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Result<Vec<BranchStatus>, String> {
    let mut rows = conn
        .query(
            "SELECT b.number, b.title, b.stage, b.moved_at, mr.github_pr_number, mr.status
         FROM thread_branches b
         LEFT JOIN merge_requests mr ON mr.job_id = b.job_id
         WHERE b.thread_id = (SELECT thread_id FROM jobs WHERE id = ?1)
         ORDER BY b.number",
            params![job_id],
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut branches = Vec::new();
    while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
        let pr_status = row.opt_text(5).map_err(|error| error.to_string())?;
        branches.push(BranchStatus {
            number: row.i64(0).map_err(|error| error.to_string())?,
            title: row.opt_text(1).map_err(|error| error.to_string())?,
            stage: row.text(2).map_err(|error| error.to_string())?,
            moved_at: row.i64(3).map_err(|error| error.to_string())?,
            pr: pr_status.map(|status| (row.opt_i64(4).ok().flatten(), status)),
        });
    }
    Ok(branches)
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
    /// One gate-census line per held execution (`exec N — held, waiting on
    /// ...`). A child whose only execution is held derives Backlog and has no
    /// nodes, so without these it would read exactly like a child nobody
    /// started.
    held: Vec<String>,
    /// One census line per gated execution that can never start (`exec N —
    /// won't start: superseded by exec M`). Detail only: such an execution
    /// is not something the child waits on, so it earns no `held` marker and
    /// does not keep a failed child live.
    wont_start: Vec<String>,
}

/// Statuses that end an issue. A child in one of these is history: its turns may
/// be compacted into a chapter, and its census line carries no live detail.
const TERMINAL_ISSUE_STATUSES: [&str; 3] = ["merged", "closed", "failed"];

impl ChildStatus {
    /// Whether this child is live work rather than history. A failed child
    /// with a held rerun is waiting on its gates, not finished: it keeps its
    /// detail lines, stays out of the resolved-history cap, and keeps its
    /// turns out of era compaction.
    fn is_open(&self) -> bool {
        !TERMINAL_ISSUE_STATUSES.contains(&self.status.as_str())
            || (self.status == "failed" && !self.held.is_empty())
    }

    /// The one line that addresses a child: where it lives, where it stands,
    /// what it is. Every census renders this; what a census decides is whether
    /// an open child also gets its detail block underneath.
    fn census_line(&self) -> String {
        // The held marker rides on the address line, not the detail block, so
        // a collapsed census that drops detail still says a child is held.
        let held = if self.held.is_empty() { "" } else { " · held" };
        format!(
            "- `{}` **{}**{held} · {}\n",
            self.uri,
            self.status,
            one_line(&self.title)
        )
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
    let mut out = String::from("## Children\n\n");
    out.push_str(CHILDREN_NOTICE);
    out.push_str("\n\n");
    if children.is_empty() {
        out.push_str(NO_CHILDREN_NOTICE);
        return out;
    }

    let (open, mut terminal): (Vec<_>, Vec<_>) = children.iter().partition(|child| child.is_open());
    terminal.sort_by_key(|child| std::cmp::Reverse(child.last_activity_at.unwrap_or(0)));

    for child in open
        .into_iter()
        .chain(terminal.iter().take(CENSUS_TERMINAL_CHILDREN).copied())
    {
        out.push_str(&child.census_line());
        if child.is_open() {
            let detail = child.detail_line(now);
            if !detail.is_empty() {
                out.push_str(&format!("  {detail}\n"));
            }
            for line in child.held.iter().chain(&child.wont_start) {
                out.push_str(&format!("  {line}\n"));
            }
        }
    }
    if terminal.len() > CENSUS_TERMINAL_CHILDREN {
        out.push_str(&format!(
            "- _… and {} earlier resolved children. Their work is summarized in the eras above and listed at `cairn:~/eras`._\n",
            terminal.len() - CENSUS_TERMINAL_CHILDREN
        ));
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
             WHERE child.parent_thread_id = (SELECT thread_id FROM jobs WHERE id = ?1)
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
            held: Vec::new(),
            wont_start: Vec::new(),
        });
    }
    if children.is_empty() {
        return Ok(children);
    }

    // Held executions are read strictly, like the enumeration itself: a held
    // child produces no attention on its own, so the census line is the only
    // place a thread learns it is waiting.
    let child_ids: Vec<String> = at.keys().cloned().collect();
    let gated = crate::execution::gates::census::gated_executions_for_issues_conn(conn, &child_ids)
        .await
        .map_err(|error| error.to_string())?;
    for (issue_id, executions) in gated {
        if let Some(child) = at
            .get(&issue_id)
            .map(|index| &mut children[*index])
            // A merged or closed child's resolution already ended its work;
            // a gated execution left on it is not something to wait on, and
            // its history line already says the child is done.
            .filter(|child| !matches!(child.status.as_str(), "merged" | "closed"))
        {
            child.held = executions.held.into_iter().map(|held| held.line).collect();
            child.wont_start = executions
                .wont_start
                .into_iter()
                .map(|held| held.line)
                .collect();
        }
    }

    // The newest execution's top-level nodes. An older execution's nodes describe
    // an attempt that has been superseded, which would read as live work.
    if let Ok(mut rows) = conn
        .query(
            "SELECT j.issue_id, COALESCE(j.node_name, j.uri_segment), j.status
             FROM jobs j
             JOIN executions e ON e.id = j.execution_id
             JOIN issues child ON child.id = j.issue_id
             WHERE child.parent_thread_id = (SELECT thread_id FROM jobs WHERE id = ?1)
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
             WHERE child.parent_thread_id = (SELECT thread_id FROM jobs WHERE id = ?1)
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
             WHERE child.parent_thread_id = (SELECT thread_id FROM jobs WHERE id = ?1)
               AND p.answered_at IS NULL
               AND p.withdrawn_at IS NULL
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
             WHERE child.parent_thread_id = (SELECT thread_id FROM jobs WHERE id = ?1)
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
             WHERE child.parent_thread_id = (SELECT thread_id FROM jobs WHERE id = ?1)
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

// ── Non-thread seeds ───────────────────────────────────────────────────────────

/// Framing for the pointers. The plan state lives in these documents and not in
/// the seed, so the seed says so rather than letting a turn below stand in for it.
const WORKING_DOCUMENTS_NOTICE: &str =
    "_Your plan state lives in these, not in this seed. Re-read them before acting._";

const EARLIER_TURNS_NOTICE: &str = "_One line per stretch of conversation, each starting where a message reached you, with the address that reads it in full. Read a range only when it bears on what you are doing now: re-reading costs more than it saves unless the range is load-bearing._";

/// The share of a node seed's budget, in percent, the table of contents may
/// claim from the verbatim window once the whole history no longer fits. The
/// newest turns are the one part of a seed no address carries as cheaply, so
/// they keep the rest.
const NODE_TOC_SHARE: usize = 25;

/// Compose the seed a non-thread node's rebuilt session starts from: pointers to
/// its working documents, a table of contents of the turns that did not fit, and
/// the newest turns verbatim, all within `budget_tokens` as [`seed_tokens`]
/// measures it.
///
/// An issue's node has an objective and an ending, but one that lives long — a
/// coordinator running for days — accumulates history exactly as a thread does,
/// and a seed that carries all of it opens the next session as full as the one
/// it replaced. A node has no arc, eras, or children, and nothing here persists:
/// every composition reads the transcript afresh, so the table of contents is
/// derived each time rather than carried between generations.
///
/// `Ok(None)` when no transcript rows could be loaded. That is not evidence of an
/// empty history, so the caller settles it with a direct count.
pub(crate) async fn compose_node_seed(
    db: &LocalDb,
    job: &crate::db_records::DbJob,
    budget_tokens: usize,
) -> Result<Option<String>, String> {
    // As in the thread composer: no connection outlives the small reads.
    let events = load_job_events_ordered(db, &job.id)
        .await
        .map_err(|error| {
            format!("Cannot compose a seed: the transcript could not be read: {error}")
        })?;
    if events.is_empty() {
        return Ok(None);
    }

    let (coordinates, artifacts) = {
        let conn = connect_for_read(db).await?;
        let coordinates = node_coordinates(&conn, &job.id).await;
        let artifacts = artifact_names(&conn, &job.id)
            .await
            .map_err(|error| format!("Failed to list this node's artifacts: {error}"))?;
        (coordinates, artifacts)
    };
    let fallback_label = job
        .node_name
        .as_deref()
        .or(job.uri_segment.as_deref())
        .unwrap_or("session");
    let (home, meta) = match coordinates.as_ref() {
        Some(coordinates) => (
            coordinates.base_uri.clone(),
            DigestMeta {
                label: &coordinates.label,
                project: &coordinates.project_key,
                coordinate: coordinates.digest_coordinate(),
                status: &coordinates.status,
            },
        ),
        // Every node that can run resolves a home URI, so this is a job Cairn
        // cannot address canonically. Its agent can still reach its own
        // documents and transcript relative to itself.
        None => (
            "cairn:~".to_string(),
            DigestMeta {
                label: fallback_label,
                project: &job.project_id,
                coordinate: DigestCoordinate::Unaddressed,
                status: &job.status,
            },
        ),
    };
    let chat_uri = format!("{home}/chat");
    let documents = render_working_documents(coordinates.as_ref(), &home, &artifacts);
    let blocks = group_turn_blocks(&events);
    Ok(Some(compose_node_body(
        budget_tokens,
        &documents,
        &blocks,
        &chat_uri,
        &meta,
    )))
}

/// The documents, pointers, and turns of a node seed, fitted to `budget`.
fn compose_node_body(
    budget: usize,
    documents: &str,
    blocks: &[TurnBlock<'_>],
    chat_uri: &str,
    meta: &DigestMeta<'_>,
) -> String {
    let available = budget.saturating_sub(seed_tokens(documents));
    // A history that fits whole needs no table of contents, and holding a share
    // open for one would drop turns to protect space nothing uses.
    let whole = bounded_recent(blocks, available, chat_uri, meta, usize::MAX);
    let (recent, toc) = if whole.selected.first() == Some(&0) {
        (whole, String::new())
    } else {
        let recent = bounded_recent(
            blocks,
            available - available * NODE_TOC_SHARE / 100,
            chat_uri,
            meta,
            usize::MAX,
        );
        let toc = earlier_turns(
            blocks,
            recent_start(&recent, blocks.len()),
            chat_uri,
            available.saturating_sub(seed_tokens(&recent.section)),
        );
        (recent, toc)
    };
    let body = format!("{documents}{toc}{}", recent.section);
    if seed_tokens(&body) <= budget {
        return body;
    }

    // Sections measured apart do not quite sum to the whole, and pointers past
    // the budget leave nothing to apportion. The table of contents collapses to
    // the one line that addresses all of it, and a tokenizer cut is the last
    // boundary, so no input shape produces an over-budget seed.
    let toc = earlier_turns(blocks, recent_start(&recent, blocks.len()), chat_uri, 0);
    let mut body = format!("{documents}{toc}{}", recent.section);
    if seed_tokens(&body) > budget {
        body.truncate(seed_cut(&body, budget));
    }
    body
}

fn recent_start(recent: &BoundedRecent, total: usize) -> usize {
    recent.selected.first().copied().unwrap_or(total)
}

/// The table of contents for turns `0..end`, fitted to `budget`.
///
/// The chapters are the thread planner's, with nothing marked and nothing open,
/// so every turn below `end` lands in exactly one stretch bounded where a message
/// reached the agent. The newest stretches render one line each; everything older
/// collapses into the single marker that addresses it.
fn earlier_turns(blocks: &[TurnBlock<'_>], end: usize, chat_uri: &str, budget: usize) -> String {
    if end == 0 {
        return String::new();
    }
    let entries: Vec<TocEntry> = plan_chapters(blocks, &[], &[], &[], end)
        .chapters
        .iter()
        .map(|chapter| build_entry(chapter, blocks, &[], chat_uri))
        .collect();

    // The most entries that fit, found by binary search over the measured
    // section, as the collapsed census is. Only a measured candidate is accepted.
    let (mut low, mut high) = (0usize, entries.len());
    let mut best = 0usize;
    while low <= high {
        let mid = low + (high - low) / 2;
        if seed_tokens(&render_earlier_turns(&entries, mid, end, chat_uri)) <= budget {
            best = mid;
            low = mid + 1;
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
    render_earlier_turns(&entries, best, end, chat_uri)
}

/// The newest `take` of `entries`, after one marker for every turn older than
/// them. `entries` cover `0..end` contiguously.
fn render_earlier_turns(entries: &[TocEntry], take: usize, end: usize, chat_uri: &str) -> String {
    let mut out = String::from("## Earlier turns\n\n");
    out.push_str(EARLIER_TURNS_NOTICE);
    out.push_str("\n\n");
    let kept = &entries[entries.len() - take..];
    let collapsed_end = kept
        .first()
        .map_or(end, |entry| entry.start_block.max(0) as usize);
    if collapsed_end > 0 {
        out.push_str(&omitted_range_marker(0, collapsed_end, chat_uri));
    }
    for entry in kept {
        let (start, last) = (entry.start_block + 1, entry.end_block + 1);
        let range = if start == last {
            format!("Turn {start}")
        } else {
            format!("Turns {start}–{last}")
        };
        out.push_str(&format!(
            "- {range}: {} → `{}`\n",
            entry.overview, entry.content_uri
        ));
    }
    out.push('\n');
    out
}

/// Where the node's own working documents are: the issue (or thread) it serves,
/// its todos, and every artifact it has written.
fn render_working_documents(
    coordinates: Option<&NodeCoordinates>,
    home: &str,
    artifacts: &[String],
) -> String {
    let mut out = String::from("## Working documents\n\n");
    out.push_str(WORKING_DOCUMENTS_NOTICE);
    out.push_str("\n\n");
    if let Some(coordinates) = coordinates {
        match coordinates.thread_name.as_deref() {
            Some(name) => out.push_str(&format!(
                "- Thread: `{}`\n",
                cairn_common::uri::build_thread_uri(&coordinates.project_key, name)
            )),
            None if coordinates.number > 0 => out.push_str(&format!(
                "- Issue: `{}`\n",
                cairn_common::uri::build_issue_uri(&coordinates.project_key, coordinates.number)
            )),
            None => {}
        }
    }
    out.push_str(&format!("- Todos: `{home}/todos`\n"));
    for name in artifacts {
        out.push_str(&format!("- Artifact `{name}`: `{home}/{name}`\n"));
    }
    out.push('\n');
    out
}

async fn artifact_names(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Result<Vec<String>, String> {
    let mut rows = conn
        .query(
            "SELECT DISTINCT output_name FROM artifacts
             WHERE job_id = ?1 AND output_name IS NOT NULL AND output_name != ''
             ORDER BY output_name",
            params![job_id],
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut names = Vec::new();
    while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
        names.push(row.text(0).map_err(|error| error.to_string())?);
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::jobs::DigestPreview;
    use crate::storage::migrated_test_db;

    const THREAD_JOB: &str = "job-thread";
    const DONE_CHILD: &str = "cairn://p/prj/20";
    const OPEN_CHILD: &str = "cairn://p/prj/21";

    /// Composition time for every test. The fixture's newest event is at 141, so
    /// a child's last activity reads as a couple of hours old.
    const NOW: i64 = 10_000;

    #[test]
    fn branch_census_keeps_active_and_caps_terminal_branches() {
        let mut branches = vec![BranchStatus {
            number: 1,
            title: Some("Live work".into()),
            stage: "pr-open".into(),
            moved_at: NOW - 120,
            pr: Some((Some(42), "open".into())),
        }];
        for number in 2..=(CENSUS_TERMINAL_BRANCHES as i64 + 2) {
            branches.push(BranchStatus {
                number,
                title: None,
                stage: "merged".into(),
                moved_at: NOW - number,
                pr: None,
            });
        }
        let rendered = render_branches(&branches, NOW);
        assert!(rendered.contains("`cairn:~/branches/1` **pr-open**"));
        assert!(rendered.contains("PR #42 open"));
        assert!(rendered.contains("and 1 earlier terminal branches"));
    }

    #[test]
    fn store_census_is_one_line_and_never_contains_rows() {
        let rendered = render_thread_store_census(
            &crate::threads::db_store::ThreadDbCensus {
                tables: vec![("declined".to_string(), 1_284), ("sources".to_string(), 6)],
                bytes: 1_258_291,
                last_write: Some(NOW - 3 * 60 * 60),
            },
            NOW,
        );

        assert_eq!(
            rendered,
            "- **Store.** `cairn:~/db` — declined 1,284 rows, sources 6 rows · 1.2 MB · last write 3h ago.\n\n"
        );
        assert_eq!(rendered.trim().lines().count(), 1);
        assert!(!rendered.contains("SELECT"));
        assert!(!rendered.contains("https://"));
    }

    #[test]
    fn store_census_bounds_and_flattens_hostile_table_names() {
        let mut tables = (0..20)
            .map(|index| (format!("table-{index}-{}", "x".repeat(100)), index))
            .collect::<Vec<_>>();
        tables[0].0 = "hostile\nname\rwith\tcontrols".to_string();
        let rendered = render_thread_store_census(
            &crate::threads::db_store::ThreadDbCensus {
                tables,
                bytes: 0,
                last_write: None,
            },
            NOW,
        );
        assert_eq!(rendered.trim().lines().count(), 1);
        assert!(rendered.contains("hostile name with controls 0 rows"));
        assert!(rendered.contains("… 12 more tables → `cairn:~/db`"));
        assert!(
            rendered.len() < 700,
            "bounded census was {} bytes",
            rendered.len()
        );
    }

    fn tenet(number: i64, status: TenetStatus, text: &str) -> TenetView {
        use crate::tenets::{AnswerSource, TenetRow};
        let answered = status.has_verdict();
        let row = TenetRow {
            id: format!("t{number}"),
            project_id: "p".into(),
            number,
            proposing_thread_id: "thread".into(),
            slug: None,
            statement: text.to_string(),
            status,
            verdict: answered.then_some(if status == TenetStatus::Declined {
                crate::tenets::Verdict::No
            } else {
                crate::tenets::Verdict::Yes
            }),
            comment: None,
            answered_by: None,
            answer_source: answered.then_some(AnswerSource::Device),
            answered_at: answered.then_some(1_700_000_000 + number),
            supersedes: None,
            origin: TenetOrigin::Standing,
            legacy: None,
            contradiction_alerted_at: None,
            proposer: cairn_common::identity::PrincipalRef::Agent {
                node: "cairn://p/prj/topic".into(),
                run_id: None,
            },
            created_at: 1_699_000_000,
            updated_at: 1_700_000_000,
        };
        TenetView {
            uri: format!("cairn://p/prj/tenets/{number}"),
            project_key: "prj".into(),
            proposing_thread: "prj/topic".into(),
            proposing_thread_name: Some("topic".into()),
            superseded_by: None,
            answerer_label: answered.then(|| "anonymous device d-1".to_string()),
            policy_label: "Releases on: answered".into(),
            gates: Vec::new(),
            evidence: Vec::new(),
            thread_your_calls: 0,
            row,
        }
    }

    fn projection(tenets: Vec<TenetView>) -> ArcProjection {
        ArcProjection {
            prose: serde_json::json!({"current_intent": "Keep going."}),
            tenets,
            open_questions: vec![serde_json::json!({"question": "What next?"})],
            thread_id: "thread".into(),
            project_id: "p".into(),
            project_key: "prj".into(),
            thread_name: "topic".into(),
        }
    }

    fn render_tenets(tenets: Vec<TenetView>) -> String {
        let ranked = recency_rank(&tenets);
        render_arc_projection(&projection(tenets), &ranked)
    }

    #[test]
    fn an_approved_tenet_renders_in_full_with_its_verdict() {
        let mut successor = tenet(4, TenetStatus::Approved, "Ship the lease change");
        successor.row.comment = Some("Do it this week".into());
        successor.row.supersedes = Some(2);
        let rendered = render_tenets(vec![successor]);
        assert!(
            rendered.contains(
                "- **#4** (approved by anonymous device d-1, 2023-11-14) — Ship the lease change\n  comment: Do it this week\n  supersedes #2\n"
            ),
            "{rendered}"
        );
    }

    #[test]
    fn a_migrated_tenet_keeps_its_slug_and_says_it_was_migrated() {
        let mut migrated = tenet(1, TenetStatus::Approved, "No budget kills");
        migrated.row.origin = TenetOrigin::MigratedRuling;
        migrated.row.slug = Some("no-budget-kills".into());
        migrated.answerer_label = Some("migrated ruling".into());
        let rendered = render_tenets(vec![migrated]);
        assert!(
            rendered.contains("- **#1** (approved by migrated ruling, 2023-11-14) — No budget kills\n  slug: `no-budget-kills`\n"),
            "{rendered}"
        );
    }

    #[test]
    fn sections_render_in_order_with_the_pointer_after_the_index() {
        let rendered = render_tenets(vec![
            tenet(1, TenetStatus::Approved, "Keep leases"),
            tenet(2, TenetStatus::Declined, "Drop leases"),
            tenet(3, TenetStatus::Proposed, "Add a timeout"),
        ]);
        let order = [
            "Keep going.",
            "### Awaiting verdict",
            "### Tenets",
            "### Tenet index",
            "Full text: cairn://p/prj/tenets/<n> · all of this thread's: cairn://p/prj/tenets?thread=topic",
            "### Open questions",
        ]
        .map(|needle| {
            rendered
                .find(needle)
                .unwrap_or_else(|| panic!("missing {needle}: {rendered}"))
        });
        assert!(order.windows(2).all(|pair| pair[0] < pair[1]), "{rendered}");
        assert!(
            rendered.contains("- **#3** (proposed 2023-11-03) — Add a timeout\n"),
            "{rendered}"
        );
    }

    #[test]
    fn declined_and_superseded_tenets_always_land_in_the_index_and_withdrawn_never_render() {
        let rendered = render_tenets(vec![
            tenet(1, TenetStatus::Superseded, "Old rule"),
            tenet(2, TenetStatus::Declined, "Rejected idea"),
            tenet(3, TenetStatus::Withdrawn, "Withdrawn idea"),
        ]);
        assert!(!rendered.contains("### Tenets\n"), "{rendered}");
        assert!(
            rendered.contains("- #1 · superseded · Old rule\n"),
            "{rendered}"
        );
        assert!(
            rendered.contains("- #2 · declined · Rejected idea\n"),
            "{rendered}"
        );
        assert!(!rendered.contains("Withdrawn idea"), "{rendered}");
    }

    #[test]
    fn a_thread_without_tenets_renders_no_tenet_sections() {
        let rendered = render_tenets(vec![tenet(1, TenetStatus::Withdrawn, "Gone")]);
        for absent in [
            "### Awaiting verdict",
            "### Tenets",
            "### Tenet index",
            "Full text:",
        ] {
            assert!(!rendered.contains(absent), "{rendered}");
        }
        assert!(rendered.contains("### Open questions"));
    }

    #[test]
    fn the_budget_fills_most_recent_first_and_overflow_moves_to_the_index() {
        let long = "word ".repeat(400);
        let tenets: Vec<TenetView> = (1..=12)
            .map(|number| {
                tenet(
                    number,
                    TenetStatus::Approved,
                    &format!("Rule {number}. {long}"),
                )
            })
            .collect();
        let rendered = render_tenets(tenets);
        let full = &rendered
            [rendered.find("### Tenets").unwrap()..rendered.find("### Tenet index").unwrap()];
        assert!(
            seed_tokens(full) <= TENET_FULL_BUDGET_TOKENS + 10,
            "{}",
            seed_tokens(full)
        );
        // Most recently answered first, so the newest is in full and the oldest
        // is an index line.
        assert!(full.contains("**#12**"));
        assert!(full.find("**#12**") < full.find("**#11**"));
        assert!(!full.contains("**#1**"));
        assert!(
            rendered.contains("- #1 · approved · Rule 1.\n"),
            "{rendered}"
        );
    }

    #[test]
    fn an_index_line_carries_the_first_sentence_capped_at_160_characters() {
        assert_eq!(
            tenet_first_sentence("Keep it.  Then more.", 160),
            "Keep it."
        );
        assert_eq!(
            tenet_first_sentence("v1.2 ships first. Later", 160),
            "v1.2 ships first."
        );
        let capped = tenet_first_sentence(&"x".repeat(400), 160);
        assert_eq!(capped.chars().count(), 160);
        assert!(capped.ends_with('…'));
        let mut slugged = tenet(7, TenetStatus::Declined, "y");
        slugged.row.slug = Some("old-handle".into());
        assert!(render_index_tenet(&slugged).ends_with(" [old-handle]\n"));
    }

    #[test]
    fn volatile_working_state_is_carried_beside_durable_intent() {
        let data = serde_json::json!({
            "current_intent": "Keep the durable direction.",
            "working": "Ratify A, then watch B, then start C."
        });
        let rendered = render_arc_data(&data);
        assert!(rendered.starts_with("Keep the durable direction."));
        assert!(rendered.contains("### Working (volatile)"));
        assert!(rendered.contains("Ratify A, then watch B, then start C."));
    }

    /// A thread with fourteen turns: four old ones behind the recency window,
    /// then ten recent. Of the four old turns, one is plain conversation, two
    /// discuss a child that has since merged, and one touches a child that is
    /// still open.
    async fn thread_fixture(name: &str) -> LocalDb {
        let db = migrated_test_db(name).await;
        // Every fixture shares the thread id, and the redirect is process-wide,
        // so they share one temporary store rather than the real thread-database
        // path. A per-fixture path would move under a parallel test mid-compose.
        static STORE: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
        crate::threads::db_store::use_test_path(
            "t-thread",
            STORE
                .get_or_init(|| tempfile::tempdir().unwrap().keep().join("t-thread.db"))
                .clone(),
        );
        // A thread mints its store at creation; minting it here keeps a
        // parallel fixture's first open from changing the store census between
        // two composes of another test.
        crate::threads::db_store::open("t-thread").await.unwrap();
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','prj','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i-thread','p',1,'Platform thread','active','none',1,1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('t-thread','p','platform','active','none',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at, parent_thread_id)
               VALUES ('i-done','p',20,'Running panel for the executor fleet','merged','none',1,1,'t-thread');
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at, parent_thread_id)
               VALUES ('i-open','p',21,'Lease renewal backoff','active','none',1,1,'t-thread');
             INSERT INTO executions(id, recipe_id, project_id, status, started_at, seq)
               VALUES ('e','recipe','p','running',1,1);
             INSERT INTO jobs(id, execution_id, thread_id, project_id, status, uri_segment, node_name, current_session_id, created_at, updated_at)
               VALUES ('job-thread','e','t-thread','p','running','thread','thread','sess-1',1,1);
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
                "provenance": ["cairn://p/prj/20"]
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
    async fn a_composed_seed_uses_eras_and_a_token_budget_without_rendering_chapters() {
        let db = thread_fixture("thread-seed-compose.db").await;
        mark_done_child(&db).await;
        write_arc(&db, 1, "Getting the fleet onto lease-based residency.").await;

        // A budget the composed seed can actually honor. At 300 this fixture
        // costs more than the bound it is given, so what it demonstrated was the
        // gap that bound had rather than the policy inside it.
        let seed = compose_thread_seed_with_policy(
            &db,
            THREAD_JOB,
            NOW,
            SeedPolicy {
                budget_tokens: 600,
                inline_eras: 3,
            },
        )
        .await
        .unwrap();

        assert!(seed
            .content
            .contains("Getting the fleet onto lease-based residency."));
        // An embedded arc ruling is a tenet once the boot import runs; the seed
        // never renders it from the arc itself.
        assert!(!seed.content.contains("Do not rebuild the fleet view"));
        assert!(seed.content.contains("Who owns lease renewal?"));
        assert!(seed.content.contains("OPEN_CHILD_TURN_FOUR"));
        assert!(seed.content.contains("## Turn 14 ·"));
        assert!(!seed.content.contains("## History"));
        assert!(!seed.content.contains("- turn 1 ·"));
        assert_eq!(seed.recency_start_block, 11);
        assert_eq!(seed.minted_eras.len(), 1);
        assert!(seed.minted_eras[0]
            .children
            .iter()
            .any(|child| child.child_issue_id == "i-done"));
        assert_eq!(seed.consumed_child_issue_ids, vec!["i-done".to_string()]);
    }

    /// Give the open child everything a live child accumulates: a running
    /// execution, a pull request, a red check beside green ones, an unanswered
    /// question, and recent activity.
    async fn make_open_child_live(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
               VALUES ('e-open-old','build','i-open','p','complete',1,1,'superseded');
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

    /// A child whose only execution is held derives Backlog and has no nodes.
    /// The census must still say it is held and name what it waits on, or the
    /// thread reads it as a child nobody started.
    #[tokio::test]
    async fn the_census_names_a_held_childs_gates_and_their_evidence() {
        use crate::execution::gates::census::test_support::{hold_execution, HELD_SUMMARY};
        let db = thread_fixture("thread-seed-held-child.db").await;
        db.execute_script(
            "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at, parent_thread_id)
               VALUES ('i-held','p',22,'Rerun after the fix','backlog','none',1,1,'t-thread');
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e-held','build','i-held','p','held',3,1);",
        )
        .await
        .unwrap();
        hold_execution(&db, "e-held").await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();
        assert!(
            seed.content.contains(&format!(
                "- `cairn://p/prj/22` **backlog** · held · Rerun after the fix\n  exec 1 — {HELD_SUMMARY}\n"
            )),
            "the held child's census is wrong: {}",
            seed.content
        );
    }

    /// A failed child with a held rerun is waiting, not history: it keeps its
    /// gate lines and is never cut by the resolved-history cap, however many
    /// finished children sort ahead of it.
    #[tokio::test]
    async fn a_failed_child_with_a_held_rerun_is_live_not_history() {
        use crate::execution::gates::census::test_support::{hold_execution, HELD_SUMMARY};
        let db = thread_fixture("thread-seed-failed-held.db").await;
        let mut script = String::new();
        for n in 0..(CENSUS_TERMINAL_CHILDREN as i64 + 2) {
            script.push_str(&format!(
                "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at, parent_thread_id)
                   VALUES ('i-old-{n}','p',{},'Old work {n}','merged','none',1,1,'t-thread');\n",
                100 + n
            ));
        }
        script.push_str(
            "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at, parent_thread_id)
               VALUES ('i-retry','p',500,'Retry once green','failed','none',1,1,'t-thread');
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
               VALUES ('e-failed','build','i-retry','p','failed',1,1,'superseded');
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e-rerun','build','i-retry','p','held',2,2);",
        );
        db.execute_script(&script).await.unwrap();
        hold_execution(&db, "e-rerun").await;

        let seed = compose_thread_seed(&db, THREAD_JOB, NOW).await.unwrap();
        assert!(
            seed.content.contains(&format!(
                "- `cairn://p/prj/500` **failed** · held · Retry once green\n  exec 2 — {HELD_SUMMARY}\n"
            )),
            "the failed+held child lost its census: {}",
            seed.content
        );
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
                "- `cairn://p/prj/21` **active** · Lease renewal backoff\n  \
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
                "- `cairn://p/prj/20` **merged** · Running panel for the executor fleet\n"
            ),
            "the merged child's census line is wrong: {}",
            seed.content
        );

        // The census sits between authored intent and persisted eras.
        let arc = seed.content.find("## Arc").expect("an arc section");
        let children = seed
            .content
            .find("## Children")
            .expect("a children section");
        let eras = seed.content.find("## Eras").expect("an eras section");
        assert!(arc < children && children < eras);
    }

    /// The part of a composed seed the budget governs. The authored arc is
    /// copied verbatim and is deliberately exempt, so measuring it here would be
    /// measuring the one thing the bound does not claim.
    fn non_arc_body(content: &str) -> &str {
        // Whichever generated section comes first. Under a budget too small for
        // the census the body opens at a later header, or is empty outright.
        ["## Children", "## Eras", "## Recent"]
            .iter()
            .filter_map(|header| content.find(header))
            .min()
            .map_or("", |at| &content[at..])
    }

    /// Live open children, each carrying the facets a live child accumulates.
    /// A census line without them is not the line that costs anything.
    async fn many_live_open_children(db: &LocalDb, count: i64) {
        for n in 0..count {
            let issue = format!("i-many-{n}");
            let number = 100 + n;
            db.execute_script(&format!(
                "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at, parent_thread_id)
                   VALUES ('{issue}','p',{number},'Lease renewal backoff for executor {number}, with the retry curve still unsettled','active','none',1,1,'t-thread');
                 INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                   VALUES ('e-{issue}','build','{issue}','p','running',1,1);
                 INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                   VALUES ('job-{issue}','e-{issue}','{issue}','p','running','builder','builder',1,1);
                 INSERT INTO runs(id, job_id, issue_id, status, created_at, updated_at)
                   VALUES ('run-{issue}','job-{issue}','{issue}','running',1,1);
                 INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, github_pr_number)
                   VALUES ('mr-{issue}','job-{issue}','p','{issue}','Lease renewal backoff','feat/lease','main','open',10,10,{number});
                 INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                   VALUES ('ev-{issue}','run-{issue}',1,9100,'assistant','{{}}',9100);"
            ))
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn a_census_too_large_for_the_budget_collapses_without_losing_a_child() {
        // The census is framing every rebuilt turn pays for, so an unbounded one
        // is unbounded forever. Twenty live children in full detail outweigh
        // this budget; the same twenty as one addressable line each do not.
        let db = thread_fixture("thread-seed-oversized-census.db").await;
        many_live_open_children(&db, 20).await;
        write_arc(&db, 1, "Getting the fleet onto lease-based residency.").await;

        let policy = SeedPolicy {
            budget_tokens: 1_500,
            inline_eras: 3,
        };
        let seed = compose_thread_seed_with_policy(&db, THREAD_JOB, NOW, policy)
            .await
            .unwrap();
        let body = non_arc_body(&seed.content);

        // Measured the way production measures, not by a bytes-over-four guess.
        assert!(
            seed_tokens(body) <= policy.budget_tokens,
            "the composed seed exceeded its own budget at {} tokens:\n{body}",
            seed_tokens(body)
        );
        // Collapsing costs detail, never a child: each one still carries the URI
        // the detail is one read away at.
        for number in 100..120 {
            assert!(
                body.contains(&format!("`cairn://p/prj/{number}`")),
                "child {number} lost its address in the collapsed census:\n{body}"
            );
        }
        assert!(body.contains("carries addresses where it would otherwise carry detail"));
        assert!(
            !body.contains("PR #100 open"),
            "a collapsed child kept the detail block the budget could not afford"
        );

        // Without this, a fixture that was never oversized would pass silently.
        let generous = compose_thread_seed_with_policy(
            &db,
            THREAD_JOB,
            NOW,
            SeedPolicy {
                budget_tokens: 100_000,
                inline_eras: 3,
            },
        )
        .await
        .unwrap();
        assert!(
            seed_tokens(non_arc_body(&generous.content)) > policy.budget_tokens,
            "the fixture fits the budget on its own, so the boundary went untested"
        );
        assert!(
            generous.content.contains("PR #100 open"),
            "a seed that fits must keep the richer census exactly as it was"
        );
    }

    #[tokio::test]
    async fn a_newest_turn_larger_than_the_budget_is_cut_rather_than_dropped() {
        // One turn carrying a large read outweighs the whole budget. The recency
        // floor would pass it straight through, and dropping it would hand the
        // thread a seed with no present in it.
        let db = thread_fixture("thread-seed-oversized-turn.db").await;
        write_arc(&db, 1, "Getting the fleet onto lease-based residency.").await;
        turn(
            &db,
            15,
            "read the fleet snapshot",
            Some(&format!(
                "SNAPSHOT_HEAD\n{}",
                "lease residency row for one executor. ".repeat(2_000)
            )),
        )
        .await;

        let policy = SeedPolicy {
            budget_tokens: 800,
            inline_eras: 3,
        };
        let seed = compose_thread_seed_with_policy(&db, THREAD_JOB, NOW, policy)
            .await
            .unwrap();
        let body = non_arc_body(&seed.content);

        assert!(
            seed_tokens(body) <= policy.budget_tokens,
            "one oversized turn carried the seed past its budget at {} tokens",
            seed_tokens(body)
        );
        assert!(
            body.contains("SNAPSHOT_HEAD"),
            "the turn this seed is centered on is not in it:\n{body}"
        );
        assert!(
            body.contains("Turn 15 continues → "),
            "the cut left no address for the rest of the turn:\n{body}"
        );
        assert_eq!(
            seed.recency_start_block, 14,
            "the recorded window must describe the turns the seed actually kept"
        );
    }

    #[tokio::test]
    async fn a_budget_below_its_own_framing_falls_back_and_still_holds() {
        // The terminal boundary, reached when the section headers that introduce
        // a seed do not themselves fit. Nothing here survives as content worth
        // reading, which is exactly why the only thing asserted is the bound: it
        // is the one property that must hold for every input, including inputs no
        // caller should ever pass.
        let db = thread_fixture("thread-seed-budget-below-framing.db").await;
        make_open_child_live(&db).await;
        write_arc(&db, 1, "Getting the fleet onto lease-based residency.").await;

        // Below the tokenizer's own message framing no string can qualify, the
        // empty one included, so the sweep starts where the claim is meaningful.
        for budget in [seed_tokens(""), 12, 30, 60, 120, 240] {
            let seed = compose_thread_seed_with_policy(
                &db,
                THREAD_JOB,
                NOW,
                SeedPolicy {
                    budget_tokens: budget,
                    inline_eras: 3,
                },
            )
            .await
            .unwrap();
            let body = non_arc_body(&seed.content);
            assert!(
                seed_tokens(body) <= budget,
                "a budget of {budget} composed a {}-token body:\n{body}",
                seed_tokens(body)
            );
            // The arc is exempt, so it survives a budget that leaves nothing else.
            assert!(seed
                .content
                .contains("Getting the fleet onto lease-based residency."));
        }
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
    async fn a_thread_with_no_children_carries_an_explicit_empty_census() {
        // Silence made the production join bug unfalsifiable. Empty is data.
        let db = migrated_test_db("thread-seed-childless.db").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','prj','/tmp/p',1,1);
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

        assert!(seed.content.contains("## Children"));
        assert!(seed.content.contains("This thread has no children"));
    }

    #[tokio::test]
    async fn the_preview_measures_the_composed_seed_and_not_what_it_drops() {
        // A composition carries three plausible sizes: the seed it produces, the
        // bytes it drops (`source_bytes`), and what those dropped turns weigh as
        // chapters (`candidate_bytes`). Only the first answers "how big will this
        // thread be after I compact it", which is the question the menu is asked.
        let db = thread_fixture("thread-seed-preview-boundary.db").await;
        mark_done_child(&db).await;
        write_arc(&db, 1, "Getting the fleet onto lease-based residency.").await;

        let seed = compose_thread_seed_with_policy(
            &db,
            THREAD_JOB,
            NOW,
            SeedPolicy {
                budget_tokens: 300,
                inline_eras: 3,
            },
        )
        .await
        .unwrap();
        let preview = DigestPreview::of(&seed.content);

        // Cairn's ~4-characters-per-token convention, rounded up so a short seed
        // never reads as zero tokens.
        let characters = seed.content.chars().count() as i64;
        assert_eq!(preview.estimated_tokens, (characters + 3) / 4);

        assert!(
            seed.source_bytes > 0 && seed.candidate_bytes > 0,
            "the fixture must actually drop turns for this comparison to mean anything"
        );
        assert_ne!(preview.estimated_tokens, (seed.source_bytes + 3) / 4);
        assert_ne!(preview.estimated_tokens, (seed.candidate_bytes + 3) / 4);
    }

    #[tokio::test]
    async fn the_projected_size_follows_the_arc_it_copies() {
        // The arc is copied into every seed verbatim, so authoring more of one is
        // exactly the kind of sprawl the number exists to make visible.
        let db = thread_fixture("thread-seed-preview-arc.db").await;
        mark_done_child(&db).await;

        write_arc(&db, 1, "Short intent.").await;
        let small = DigestPreview::of(
            &compose_thread_seed(&db, THREAD_JOB, NOW)
                .await
                .unwrap()
                .content,
        );

        write_arc(&db, 2, &"A much longer statement of intent. ".repeat(40)).await;
        let large = DigestPreview::of(
            &compose_thread_seed(&db, THREAD_JOB, NOW)
                .await
                .unwrap()
                .content,
        );

        assert!(
            large.estimated_tokens > small.estimated_tokens,
            "a longer arc must project a larger seed: {} vs {}",
            large.estimated_tokens,
            small.estimated_tokens
        );
    }

    #[tokio::test]
    async fn the_projected_size_follows_the_transcript() {
        // A new turn both enters the recency window and pushes the oldest kept
        // turn out of it, so the seed a compaction would create is a different
        // one and the number must move with it.
        let db = thread_fixture("thread-seed-preview-transcript.db").await;
        mark_done_child(&db).await;
        write_arc(&db, 1, "Getting the fleet onto lease-based residency.").await;

        let before = DigestPreview::of(
            &compose_thread_seed(&db, THREAD_JOB, NOW)
                .await
                .unwrap()
                .content,
        );

        turn(
            &db,
            15,
            "One more recent turn, with rather more to say than the ones before it.",
            Some("AND_A_REPLY_TO_MATCH"),
        )
        .await;
        let after = DigestPreview::of(
            &compose_thread_seed(&db, THREAD_JOB, NOW)
                .await
                .unwrap()
                .content,
        );

        assert_ne!(
            after.estimated_tokens, before.estimated_tokens,
            "a new turn left the projected seed size unchanged"
        );
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

    /// Paging the transcript load changes how it is read, never what is read:
    /// the composed seed is byte-identical at three rows a page and unpaged.
    #[tokio::test]
    async fn a_thread_seed_is_byte_identical_whatever_the_transcript_page_size() {
        use super::super::transcript::PAGE_ROWS_OVERRIDE;
        let db = thread_fixture("thread-seed-paged.db").await;
        mark_done_child(&db).await;
        let paged = PAGE_ROWS_OVERRIDE
            .scope(3, compose_thread_seed(&db, THREAD_JOB, NOW))
            .await
            .unwrap();
        let unpaged = PAGE_ROWS_OVERRIDE
            .scope(usize::MAX, compose_thread_seed(&db, THREAD_JOB, NOW))
            .await
            .unwrap();
        assert_eq!(paged.content, unpaged.content);
        assert_eq!(paged.entries, unpaged.entries);
        assert_eq!(paged.source_bytes, unpaged.source_bytes);
        assert_eq!(paged.candidate_bytes, unpaged.candidate_bytes);
        assert_eq!(db.live_connections(), 0);
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
                minted_eras: first.minted_eras.clone(),
                arc_bytes: first.arc_bytes,
                eras_through_seq: first.eras_through_seq,
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
                minted_eras: first.minted_eras.clone(),
                arc_bytes: first.arc_bytes,
                eras_through_seq: first.eras_through_seq,
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
               VALUES ('p','default','P','prj','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i-thread','p',1,'Platform thread','active','none',1,1);
             INSERT INTO jobs(id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('job-thread','i-thread','p','running','thread','thread',1,1);",
        )
        .await
        .unwrap();

        assert!(compose_thread_seed(&db, THREAD_JOB, NOW).await.is_err());
    }

    #[test]
    fn the_canonical_arc_schema_declares_the_fields_this_composer_reads() {
        let schema =
            crate::output_schemas::load_embedded_preset_schema(crate::threads::ARC_ARTIFACT_NAME)
                .expect("the arc preset is embedded");
        let properties = schema["properties"]
            .as_object()
            .expect("the arc schema declares properties");
        for field in ["current_intent", "working"] {
            assert!(
                properties.contains_key(field),
                "the arc schema dropped {field}"
            );
        }
        // Decisions are tenets and open questions live in the thread store;
        // neither is an arc field.
        for field in ["rulings", "open_questions"] {
            assert!(
                !properties.contains_key(field),
                "the arc schema declares {field}"
            );
        }
    }

    #[test]
    fn an_issue_uri_is_not_matched_inside_a_longer_one() {
        // `cairn://p/prj/33` sits inside `cairn://p/prj/339`, and attributing one
        // child's turns to another would compact the wrong range.
        assert!(mentions_uri(
            "work on cairn://p/prj/33 today",
            "cairn://p/prj/33"
        ));
        assert!(mentions_uri(
            "see cairn://p/prj/33/1/builder",
            "cairn://p/prj/33"
        ));
        assert!(!mentions_uri(
            "work on cairn://p/prj/339",
            "cairn://p/prj/33"
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
    fn the_prose_fallback_ignores_embedded_collections() {
        let data = serde_json::json!({
            "rulings": [{ "text": "Park the migration", "status": "accepted" }]
        });
        assert_eq!(render_arc_data(&data), "");
    }

    // ── Relevance ranking ────────────────────────────────────────────────────────

    const RANK_NOW: i64 = 1_800_000_000;

    /// A project `prj` whose thread `topic` (the `projection()` thread) has a
    /// sibling thread `other`.
    async fn relevance_db(name: &str) -> LocalDb {
        let db = migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);
             INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('thread', 'p', 'topic', 'active', 'none', 1, 1);
             INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('t-other', 'p', 'other', 'active', 'none', 1, 1);",
        )
        .await
        .unwrap();
        db
    }

    async fn store_vector(db: &LocalDb, uri: &str, vector: &[f32]) {
        crate::embeddings::queries::upsert_resource_embedding_async(
            db,
            uri,
            &crate::embeddings::vector::to_bytes(vector),
            "cohere-embed-v4",
            vector.len() as i32,
        )
        .await
        .unwrap();
    }

    async fn other_thread_tenet(db: &LocalDb, number: i64, text: &str) {
        db.execute(
            "INSERT INTO tenets (id, project_id, number, proposing_thread_id, statement,
                 status, verdict, answer_source, answered_at, origin,
                 proposer_principal_json, created_at, updated_at)
             VALUES (?1, 'p', ?2, 't-other', ?3, 'approved', 'yes', 'device',
                 10, 'standing', '{\"kind\":\"agent\",\"node\":\"cairn://p/prj/other\",\"run_id\":null}', 1, 1)",
            params![format!("other-{number}"), number, text],
        )
        .await
        .unwrap();
    }

    async fn rank_with(db: &LocalDb, own: &[TenetView]) -> Vec<(i64, bool)> {
        RelevanceSelector
            .rank(
                db,
                &TenetSelection {
                    thread_id: "thread",
                    thread_name: "topic",
                    project_id: "p",
                    project_key: "prj",
                    own,
                    vectors: db,
                    now: RANK_NOW,
                },
            )
            .await
            .into_iter()
            .map(|ranked| (ranked.tenet.row.number, ranked.own))
            .collect()
    }

    fn arc_uri() -> String {
        format!(
            "{}/arc",
            cairn_common::uri::build_thread_uri("prj", "topic")
        )
    }

    fn with_recent_evidence(mut view: TenetView, at: i64) -> TenetView {
        view.evidence.push(crate::tenets::TenetEvidence {
            source_uri: "cairn://p/prj/5012".into(),
            sign: "contradicts".into(),
            confidence: 0.82,
            measured_by: Some("logprobs".into()),
            evaluated_at: at,
        });
        view
    }

    #[tokio::test]
    async fn relevance_orders_own_tenets_by_score_then_appends_near_related_ones() {
        let db = relevance_db("seed-relevance-order.db").await;
        store_vector(&db, &arc_uri(), &[1.0, 0.0, 0.0]).await;
        store_vector(&db, "cairn://p/prj/tenets/1", &[0.6, 0.8, 0.0]).await;
        store_vector(&db, "cairn://p/prj/tenets/2", &[1.0, 0.0, 0.0]).await;
        store_vector(&db, "cairn://p/prj/tenets/4", &[0.6, 0.8, 0.0]).await;
        other_thread_tenet(&db, 10, "Near rule").await;
        other_thread_tenet(&db, 11, "Far rule").await;
        store_vector(&db, "cairn://p/prj/tenets/10", &[0.8, 0.6, 0.0]).await;
        store_vector(&db, "cairn://p/prj/tenets/11", &[0.4, 0.9165, 0.0]).await;

        let own = vec![
            tenet(1, TenetStatus::Approved, "One"),
            tenet(2, TenetStatus::Approved, "Two"),
            tenet(3, TenetStatus::Approved, "Three, no vector yet"),
            // Ties #1 on similarity; recent evidence lifts it ahead.
            with_recent_evidence(tenet(4, TenetStatus::Approved, "Four"), RANK_NOW - 86_400),
            tenet(5, TenetStatus::Declined, "Five"),
        ];
        assert_eq!(
            rank_with(&db, &own).await,
            [(2, true), (4, true), (1, true), (3, true), (10, false)],
            "#11 is below the related floor; #5 is not approved"
        );

        // Old evidence is no boost, so the tie falls back to number order.
        let mut stale = own.clone();
        stale[3] = with_recent_evidence(
            tenet(4, TenetStatus::Approved, "Four"),
            RANK_NOW - 30 * 86_400,
        );
        assert_eq!(
            rank_with(&db, &stale).await[..3],
            [(2, true), (1, true), (4, true)]
        );
    }

    #[tokio::test]
    async fn an_open_child_issue_counts_as_the_threads_work() {
        let db = relevance_db("seed-relevance-child.db").await;
        db.execute_script(
            "INSERT INTO issues (id, project_id, number, title, status, parent_thread_id, created_at, updated_at)
               VALUES ('open', 'p', 40, 'Open', 'active', 'thread', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, parent_thread_id, created_at, updated_at)
               VALUES ('done', 'p', 41, 'Done', 'merged', 'thread', 1, 1);",
        )
        .await
        .unwrap();
        store_vector(&db, "cairn://p/prj/40", &[0.0, 1.0, 0.0]).await;
        store_vector(&db, "cairn://p/prj/41", &[1.0, 0.0, 0.0]).await;
        store_vector(&db, "cairn://p/prj/tenets/1", &[1.0, 0.0, 0.0]).await;
        store_vector(&db, "cairn://p/prj/tenets/2", &[0.0, 1.0, 0.0]).await;
        let own = vec![
            tenet(1, TenetStatus::Approved, "One"),
            tenet(2, TenetStatus::Approved, "Two"),
        ];
        assert_eq!(
            rank_with(&db, &own).await,
            [(2, true), (1, true)],
            "only the open child is work; the merged one is not"
        );
    }

    #[tokio::test]
    async fn with_no_work_vectors_relevance_is_recency() {
        let db = relevance_db("seed-relevance-fallback.db").await;
        store_vector(&db, "cairn://p/prj/tenets/1", &[1.0, 0.0, 0.0]).await;
        let own: Vec<TenetView> = (1..=3)
            .map(|n| tenet(n, TenetStatus::Approved, "Rule"))
            .collect();
        let recency: Vec<(i64, bool)> = recency_rank(&own)
            .into_iter()
            .map(|ranked| (ranked.tenet.row.number, ranked.own))
            .collect();
        assert_eq!(rank_with(&db, &own).await, recency);
    }

    #[tokio::test]
    async fn ranking_reads_stored_vectors_only_and_never_embeds() {
        let db = relevance_db("seed-relevance-no-network.db").await;
        store_vector(&db, &arc_uri(), &[1.0, 0.0, 0.0]).await;
        let own: Vec<TenetView> = (1..=3)
            .map(|n| tenet(n, TenetStatus::Approved, "Rule"))
            .collect();
        assert_eq!(
            rank_with(&db, &own).await,
            [(3, true), (2, true), (1, true)],
            "tenets without vectors keep recency order"
        );
        assert_eq!(
            crate::embeddings::queries::count_resource_embeddings_async(&db)
                .await
                .unwrap(),
            1
        );
    }

    #[test]
    fn related_tenets_fill_the_budget_after_own_and_never_reach_the_index() {
        let own = with_recent_evidence(
            tenet(1, TenetStatus::Approved, "Keep leases"),
            1_768_435_200,
        );
        let mut related = tenet(10, TenetStatus::Approved, "Near rule");
        related.proposing_thread = "prj/other".into();
        related.row.proposing_thread_id = "t-other".into();
        let ranked = vec![
            RankedTenet {
                tenet: own.clone(),
                own: true,
            },
            RankedTenet {
                tenet: related,
                own: false,
            },
        ];
        let rendered = render_arc_projection(
            &projection(vec![own, tenet(2, TenetStatus::Declined, "Drop leases")]),
            &ranked,
        );
        let tenets = rendered.find("### Tenets\n").unwrap();
        let related_at = rendered
            .find("### Related tenets from other threads\n")
            .unwrap_or_else(|| panic!("{rendered}"));
        let index = rendered.find("### Tenet index\n").unwrap();
        assert!(tenets < related_at && related_at < index, "{rendered}");
        assert!(
            rendered.contains(
                "- **#10** (prj/other, approved by anonymous device d-1, 2023-11-14) — Near rule\n"
            ),
            "{rendered}"
        );
        assert!(
            rendered.contains("  evidence: contradicts cairn://p/prj/5012 (0.82, 01-15)\n"),
            "{rendered}"
        );
        assert!(!rendered[index..].contains("#10"), "{rendered}");

        // With the budget spent on the thread's own tenets, nothing related fits.
        let long = "word ".repeat(2_200);
        let big = tenet(3, TenetStatus::Approved, &format!("Big. {long}"));
        let mut far = tenet(
            11,
            TenetStatus::Approved,
            &format!("Far. {}", "w ".repeat(1_200)),
        );
        far.proposing_thread = "prj/other".into();
        let rendered = render_arc_projection(
            &projection(vec![big.clone()]),
            &[
                RankedTenet {
                    tenet: big,
                    own: true,
                },
                RankedTenet {
                    tenet: far,
                    own: false,
                },
            ],
        );
        assert!(rendered.contains("- **#3** (approved"), "own fits in full");
        assert!(!rendered.contains("### Related tenets"), "{rendered}");
    }

    // ── Non-thread seeds ──

    const NODE_JOB: &str = "job-node";
    const NODE_HOME: &str = "cairn://p/prj/7/1/coordinator";

    /// A coordinator on issue 7 with a `plan` artifact and `turns` turns, each a
    /// user message and an assistant reply of `body` repeated. The newest
    /// assistant reply carries `NEWEST_TURN_MARKER`.
    async fn node_fixture(name: &str, turns: i64, body: &str, addressed: bool) -> LocalDb {
        let db = migrated_test_db(name).await;
        let issue = if addressed { "'i-node'" } else { "NULL" };
        db.execute_script(&format!(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','prj','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i-node','p',7,'Coordinate the port','active','none',1,1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e-node','recipe','i-node','p','running',1,1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, current_session_id, created_at, updated_at)
               VALUES ('job-node','e-node',{issue},'p','running','coordinator','coordinator','sess-n',1,1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('sess-n','job-node','claude','open',1,1,1);
             INSERT INTO runs(id, job_id, issue_id, status, created_at, updated_at)
               VALUES ('run-n','job-node','i-node','running',1,1);
             INSERT INTO artifacts(id, job_id, artifact_type, schema_version, data, version, output_name, created_at, updated_at, confirmed)
               VALUES ('a-plan','job-node','plan',1,'{{}}',1,'plan',1,1,1);"
        ))
        .await
        .unwrap();
        for sequence in 1..=turns {
            let turn_id = format!("turn-{sequence}");
            let assistant = if sequence == turns {
                format!("NEWEST_TURN_MARKER {body}")
            } else {
                format!("Reply {sequence}. {body}")
            };
            db.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, created_at, updated_at)
                 VALUES (?1,'sess-n','run-n','job-node',?2,?2,?2)",
                params![turn_id.as_str(), sequence],
            )
            .await
            .unwrap();
            for (offset, kind, content) in [
                (
                    0,
                    "user",
                    format!("Step {sequence}: carry on with the port. {body}"),
                ),
                (1, "assistant", assistant),
            ] {
                db.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id)
                     VALUES (?1,'run-n',?2,?2,?3,?4,?2,?5)",
                    params![
                        format!("ev-{sequence}-{kind}"),
                        sequence * 10 + offset,
                        kind,
                        serde_json::json!({ "content": content }).to_string(),
                        turn_id.as_str()
                    ],
                )
                .await
                .unwrap();
            }
        }
        db
    }

    async fn node_job(db: &LocalDb) -> crate::db_records::DbJob {
        db.read(|conn| {
            Box::pin(async move {
                crate::execution::jobs::load_job_conn(conn, NODE_JOB)
                    .await
                    .map(Option::unwrap)
            })
        })
        .await
        .unwrap()
    }

    async fn node_seed(db: &LocalDb, budget: usize) -> String {
        compose_node_seed(db, &node_job(db).await, budget)
            .await
            .unwrap()
            .expect("the node has a transcript")
    }

    /// The whole history rendered at reseed fidelity, as the retired
    /// full-history digest carried it.
    async fn whole_history_tokens(db: &LocalDb) -> usize {
        let events = load_job_events_ordered(db, NODE_JOB).await.unwrap();
        let blocks = group_turn_blocks(&events);
        let all: Vec<usize> = (0..blocks.len()).collect();
        let meta = DigestMeta {
            label: "coordinator",
            project: "prj",
            coordinate: DigestCoordinate::Unaddressed,
            status: "running",
        };
        seed_tokens(&render_blocks(&blocks, &all, "chat", &meta))
    }

    /// Tenet 1260's acceptance for digests, on a real history: composing the
    /// seed of the job with the most events leaves every checkpoint pass able
    /// to drain, no holder older than five seconds, and the log under its
    /// ceiling. Run against an APFS clone of the live database set, never the
    /// live file:
    ///
    /// ```text
    /// cp -c cairn.db cairn.db-wal cairn.db-log /tmp/probe/
    /// CAIRN_CHECKPOINT_PROBE_DB=/tmp/probe/cairn.db \
    ///   bun run test:rust -p cairn-core --lib digest_probe -- --ignored --nocapture
    /// ```
    ///
    /// `CAIRN_PROBE_JOB` picks a job instead of searching for the longest.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs a real database copy via CAIRN_CHECKPOINT_PROBE_DB"]
    async fn digest_probe_against_a_real_database_copy() {
        use crate::storage::checkpoint_probe::under_checkpoint_pressure;
        let path = std::env::var("CAIRN_CHECKPOINT_PROBE_DB")
            .expect("set CAIRN_CHECKPOINT_PROBE_DB to a copy of a real database set");
        crate::mcp::handlers::read::register_archived_file_renderer();
        let db = std::sync::Arc::new(LocalDb::open(&path).await.unwrap());
        let job_id = match std::env::var("CAIRN_PROBE_JOB") {
            Ok(job_id) => job_id,
            Err(_) => db
                .query_opt_text(
                    "SELECT r.job_id FROM events e JOIN runs r ON r.id = e.run_id \
                     GROUP BY r.job_id ORDER BY count(*) DESC LIMIT 1",
                    (),
                )
                .await
                .unwrap()
                .expect("the copy has no events"),
        };
        let events = db
            .query_opt_i64(
                "SELECT count(*) FROM events e JOIN runs r ON r.id = e.run_id WHERE r.job_id = ?1",
                (job_id.clone(),),
            )
            .await
            .unwrap()
            .unwrap_or_default();
        let job = {
            let job_id = job_id.clone();
            db.read(move |conn| {
                Box::pin(async move {
                    crate::execution::jobs::load_job_conn(conn, &job_id)
                        .await
                        .map(|job| job.expect("the probed job exists"))
                })
            })
            .await
            .unwrap()
        };
        println!("probing job {job_id}: {events} events");

        let budget = SeedPolicy::default().budget_tokens;
        let (seed, pressure) =
            under_checkpoint_pressure(db.clone(), compose_node_seed(&db, &job, budget)).await;
        let seed = seed.unwrap().unwrap_or_default();
        println!("compose_node_seed: {} seed bytes; {pressure}", seed.len());
        for pass in &pressure.passes {
            println!("  {pass}");
        }
        pressure.assert_stayed_out_of_the_way();
    }

    #[tokio::test]
    async fn a_node_seed_is_byte_identical_whatever_the_transcript_page_size() {
        use super::super::transcript::PAGE_ROWS_OVERRIDE;
        let db = node_fixture(
            "node-seed-paged.db",
            40,
            &"analysis of the parser module and its callers ".repeat(30),
            true,
        )
        .await;
        let budget = SeedPolicy::default().budget_tokens;
        let paged = PAGE_ROWS_OVERRIDE.scope(3, node_seed(&db, budget)).await;
        let unpaged = PAGE_ROWS_OVERRIDE
            .scope(usize::MAX, node_seed(&db, budget))
            .await;
        assert_eq!(paged, unpaged);
        assert_eq!(db.live_connections(), 0);
    }

    #[tokio::test]
    async fn a_node_seed_far_beyond_its_budget_keeps_its_newest_turns_a_toc_and_its_documents() {
        let db = node_fixture(
            "node-seed-bounded.db",
            200,
            &"analysis of the parser module and its callers ".repeat(30),
            true,
        )
        .await;
        let budget = SeedPolicy::default().budget_tokens;
        assert!(
            whole_history_tokens(&db).await > 10 * budget,
            "the fixture must dwarf the budget for the bound to mean anything"
        );

        let seed = node_seed(&db, budget).await;

        assert!(
            seed_tokens(&seed) <= budget,
            "{} tokens against a {budget}-token budget",
            seed_tokens(&seed)
        );
        // Working documents, as canonical addresses the agent can re-read.
        for pointer in [
            "- Issue: `cairn://p/prj/7`".to_string(),
            format!("- Todos: `{NODE_HOME}/todos`"),
            format!("- Artifact `plan`: `{NODE_HOME}/plan`"),
        ] {
            assert!(seed.contains(&pointer), "missing {pointer}: {seed}");
        }
        // The newest turn verbatim, and none of the oldest.
        assert!(seed.contains("## Turn 200 ·"), "{seed}");
        assert!(seed.contains("NEWEST_TURN_MARKER"), "{seed}");
        assert!(!seed.contains("## Turn 1 ·"), "{seed}");
        // A table of contents addressing older turns by chat offset: the newest
        // of them one line each, the rest behind the one marker that reaches it.
        let toc = &seed[seed.find("## Earlier turns").expect("a table of contents")
            ..seed.find("## Recent").expect("a verbatim window")];
        assert!(
            toc.contains(&format!("`{NODE_HOME}/chat?offset=0&limit=")),
            "{toc}"
        );
        assert!(toc.contains(": Step "), "an entry names its turn: {toc}");
        assert!(
            toc.lines()
                .filter(|line| line.starts_with("- Turn"))
                .count()
                > 3
        );
        // Sections in the order the header promises.
        let order = ["## Working documents", "## Earlier turns", "## Recent"]
            .map(|heading| seed.find(heading).unwrap());
        assert!(order.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test]
    async fn a_node_history_that_fits_is_carried_whole_without_a_toc() {
        let db = node_fixture("node-seed-whole.db", 5, "short reply", true).await;
        let seed = node_seed(&db, SeedPolicy::default().budget_tokens).await;
        for turn in 1..=5 {
            assert!(seed.contains(&format!("## Turn {turn} ·")), "{seed}");
        }
        assert!(!seed.contains("## Earlier turns"), "{seed}");
        assert!(!seed.contains("omitted"), "{seed}");
    }

    #[tokio::test]
    async fn a_node_seed_holds_a_budget_smaller_than_its_own_framing() {
        let db = node_fixture("node-seed-tiny.db", 40, &"long reply ".repeat(200), true).await;
        for budget in [40, 300, 1_500] {
            let seed = node_seed(&db, budget).await;
            assert!(
                seed_tokens(&seed) <= budget,
                "{} tokens against {budget}: {seed}",
                seed_tokens(&seed)
            );
        }
    }

    #[tokio::test]
    async fn an_unaddressable_node_points_at_itself_relatively() {
        let db = node_fixture("node-seed-relative.db", 60, &"reply ".repeat(300), false).await;
        let seed = node_seed(&db, 4_000).await;
        assert!(seed.contains("- Todos: `cairn:~/todos`"), "{seed}");
        assert!(seed.contains("`cairn:~/chat?offset=0&limit="), "{seed}");
        assert!(!seed.contains("- Issue:"), "{seed}");
    }

    #[tokio::test]
    async fn a_node_with_no_transcript_composes_nothing() {
        let db = node_fixture("node-seed-empty.db", 0, "", true).await;
        assert!(compose_node_seed(&db, &node_job(&db).await, 12_000)
            .await
            .unwrap()
            .is_none());
    }
}
