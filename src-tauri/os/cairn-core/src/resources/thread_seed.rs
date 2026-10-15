//! Composing the seed a thread session resumes from (CAIRN-3388).
//!
//! An ordinary issue reseeds from its whole transcript at full fidelity, which
//! is right for work with an objective and an ending. A thread has neither, so
//! the same digest gets more expensive every week while carrying less of what
//! matters. This composer bounds every generated part of a rebuilt seed while
//! leaving the authored arc verbatim and budget-exempt. It renders, in order:
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

use crate::storage::{LocalDb, RowExt};
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

fn seed_tokens(text: &str) -> usize {
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

const ARC_MISSING_NOTICE: &str = "_No arc has been authored for this thread yet. Write one to `cairn:~/arc` — decisions, the paths you rejected and why, and the questions still open, each with the URI it came from. It is the only part of this context that cannot be regenerated from the transcript, so nothing else in this seed substitutes for it._";

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
    let children = load_children(&conn, job_id)
        .await
        .map_err(|error| format!("Failed to resolve this thread's children: {error}"))?;
    let branches = load_branches(&conn, job_id)
        .await
        .map_err(|error| format!("Failed to resolve this thread's branches: {error}"))?;
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

    let arc = render_arc(db, job_id).await;
    let branches_census = render_branches(&branches, now);
    let census = render_children(&children, now);
    let store = thread_store_census(&conn, job_id, now).await;

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
    let recency_start = blocks.len().saturating_sub(RECENCY_FLOOR_TURNS);
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
    let mut out = String::new();
    let mut start = None;
    for index in 0..total {
        if selected.contains(&index) {
            if let Some(gap_start) = start.take() {
                out.push_str(&format!(
                    "_Turns {}–{} omitted → `{chat_uri}?offset={gap_start}&limit={}`._\n\n",
                    gap_start + 1,
                    index,
                    index - gap_start
                ));
            }
        } else if start.is_none() {
            start = Some(index);
        }
    }
    if let Some(gap_start) = start {
        out.push_str(&format!(
            "_Turns {}–{} omitted → `{chat_uri}?offset={gap_start}&limit={}`._\n\n",
            gap_start + 1,
            total,
            total - gap_start
        ));
    }
    out
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
    let recent = bounded_recent(blocks, budget.saturating_sub(framing), chat_uri, meta);

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
    let recent = bounded_recent(blocks, budget, chat_uri, meta);
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

/// The newest turns that fit `budget`, never fewer than one.
///
/// The recency floor is a ceiling here. Degradation happens because something
/// did not fit, so this takes newest-first and stops at the first turn that
/// would not — and when even the newest turn is bigger than the budget, keeps
/// its head and says where the rest of it is. An over-budget seed and an empty
/// one are both worse than a turn a thread can see was cut.
fn bounded_recent(
    blocks: &[TurnBlock<'_>],
    budget: usize,
    chat_uri: &str,
    meta: &DigestMeta<'_>,
) -> BoundedRecent {
    let Some(newest) = blocks.len().checked_sub(1) else {
        return BoundedRecent {
            section: String::from("## Recent\n\n"),
            selected: Vec::new(),
        };
    };

    let mut selected = vec![newest];
    for index in (blocks.len().saturating_sub(RECENCY_FLOOR_TURNS)..newest).rev() {
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

async fn render_arc(db: &LocalDb, job_id: &str) -> String {
    match crate::artifacts::queries::get_named(db, job_id, ARC_ARTIFACT_NAME).await {
        Ok(Some(artifact)) => {
            match crate::threads::arc_store::render(db, job_id, &artifact.data).await {
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

/// Render the authored arc as stable Markdown, copying every authored string
/// exactly. Anything this does not recognize is passed through rather than
/// dropped: the compactor is not allowed to decide that authored intent was
/// unimportant.
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
    if let Some(slug) = ruling.get("slug").and_then(|value| value.as_str()) {
        line.push_str(&format!("  - slug: `{slug}`\n"));
    }
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
}

/// Statuses that end an issue. A child in one of these is history: its turns may
/// be compacted into a chapter, and its census line carries no live detail.
const TERMINAL_ISSUE_STATUSES: [&str; 3] = ["merged", "closed", "failed"];

impl ChildStatus {
    fn is_open(&self) -> bool {
        !TERMINAL_ISSUE_STATUSES.contains(&self.status.as_str())
    }

    /// The one line that addresses a child: where it lives, where it stands,
    /// what it is. Every census renders this; what a census decides is whether
    /// an open child also gets its detail block underneath.
    fn census_line(&self) -> String {
        format!(
            "- `{}` **{}** · {}\n",
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

    #[test]
    fn ruling_slug_survives_into_the_rebuilt_seed() {
        let rendered = render_arc_data(&serde_json::json!({"rulings": [{
            "slug": "no-budget-kills", "text": "No budget kills", "status": "accepted",
            "rationale": "Quality wins", "provenance": ["cairn://p/cairn/3404"]
        }]}));
        assert!(rendered.contains("slug: `no-budget-kills`"));
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
        assert!(seed.content.contains("Do not rebuild the fleet view"));
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
        for field in ["current_intent", "rulings", "open_questions"] {
            assert!(
                properties.contains_key(field),
                "the arc schema dropped field"
            );
        }
        let ruling_required = properties["rulings"]["items"]["required"]
            .as_array()
            .expect("a ruling declares required fields");
        for field in ["text", "status", "rationale", "provenance"] {
            assert!(ruling_required
                .iter()
                .any(|value| value.as_str() == Some(field)));
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
