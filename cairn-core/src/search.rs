//! Full-text search using a local Tantivy index fed by database outbox rows.
//!
//! The index answers in documents; this module answers in PLACES. Issues,
//! comments, artifacts, and messages are already places, so each hit becomes one
//! row. Transcript events are not: a single event is a fragment of a
//! conversation, and a discussion about anything spreads its vocabulary across
//! adjacent events. So event hits are merged into HOTSPOTS — one row per stretch
//! of one node's conversation, carrying the peak hit's excerpt, how many matches
//! fell inside the stretch, and a link to the turn that excerpt came from.
//! That merge is the other half of the scored disjunction in
//! `cairn_db::storage::search_index`: the query lets partial matches through,
//! and the span re-concentrates them where they actually cluster.
//!
//! Two lanes retrieve. The text index matches words. The semantic lane
//! (`crate::embeddings::turns`) matches meaning, so a question asked in
//! different vocabulary than the conversation used still finds it. Both produce
//! transcript hits in the SAME shape and go through the SAME span merge, so
//! the two are fused as ranked lists of PLACES rather than of raw hits. When
//! the semantic lane cannot answer — no gateway token, no vectors, a filter it
//! cannot express, or a query the text index could not match — the text lane's
//! answer is returned untouched.

use crate::embeddings::turns::{turn_excerpts, SemanticSearch};
use crate::models::{SearchContentType, SearchFilters, SearchResult};
use crate::storage::{DbError, LocalDb, RowExt, SearchIndex, SearchIndexHit};
use cairn_common::uri::{
    build_issue_messages_uri, build_issue_uri, build_node_artifact_uri, build_node_chat_turn_uri,
    build_node_chat_uri, build_project_messages_uri, build_project_uri, build_task_artifact_uri,
    build_task_chat_turn_uri, build_task_chat_uri,
};
use std::collections::HashMap;

/// Ceiling the search index enforces on a single query.
const MAX_INDEX_LIMIT: usize = 100;

/// Multiple of the caller's limit fetched from the index when transcript hits
/// can be in the answer. Span merging is lossy in row count — one dense
/// conversation can absorb a dozen hits — so asking for exactly `limit`
/// documents would return a page of two or three hotspots.
const SPAN_CANDIDATE_FACTOR: usize = 4;

/// How far apart two turns may sit and still belong to the same hotspot. A
/// stretch of conversation stays one place across a turn that happens not to
/// match, which is the common shape: the operator's question, an intervening
/// tool turn, the answer.
const SPAN_TURN_GAP: i32 = 2;

/// Weight each non-peak hit contributes to a span's rank. A stretch where the
/// query's words recur outranks a lone strong hit, without letting sheer volume
/// bury a precise match.
const SPAN_ACCRUAL: f64 = 0.25;

/// Reciprocal-rank-fusion damping. The conventional 60: large enough that the
/// top few positions of a lane are not overwhelmingly favored over the next
/// few, small enough that deep positions still fade.
const RRF_DAMPING: f64 = 60.0;

// The lanes are fused with EQUAL weight, which is reciprocal-rank fusion's
// whole premise: you do not know in advance which retriever is right, so you
// reward agreement instead of picking a favorite. Down-weighting the semantic
// lane was tried and measured against real history, and it was wrong twice
// over. It buried genuinely excellent semantic hits beneath text hits that had
// matched only the stopwords "the" and "thing" — scored disjunction lets those
// through by design. And its stated justification, that inferential evidence
// should not outrank literal evidence, was really an argument about relevance,
// which the text-lane gate above now answers properly.

/// Characters of a transcript excerpt the semantic lane renders, matching what
/// the text index falls back to when it has no highlighted snippet.
const EXCERPT_CHARS: usize = 150;

/// The semantic lane's two dependencies at a search call site.
///
/// `vectors` is the PRIVATE database, which is not necessarily the database
/// holding the rows a hit resolves against: span vectors live beside
/// `resource_embeddings`, while a team project's transcripts live in its
/// replica.
pub struct SemanticLane<'a> {
    pub search: &'a SemanticSearch,
    pub vectors: &'a LocalDb,
}

/// Navigation coordinates for one hit's job.
///
/// `node_segment` is `jobs.uri_segment`, NOT the display `jobs.node_name`: URIs
/// and app routes address `builder`, while the job row's name reads `Builder`.
/// For a sub-agent task the addressable coordinate is the PARENT node plus a
/// task segment, so a task hit resolves at `.../{node}/task/{task}/...`.
///
/// A thread's work has no issue and no execution: it is addressed by the
/// thread's name directly (`cairn://p/CAIRN/general/chat/turn/3`), which
/// `thread_name` carries. Threads are where most "remember when we were talking
/// about x" conversations actually live, so a thread hit that fell back to the
/// bare project URI would miss the whole point.
#[derive(Debug, Clone, Default)]
struct JobNav {
    node_segment: Option<String>,
    task_segment: Option<String>,
    thread_name: Option<String>,
    exec_seq: Option<i32>,
    /// The job's first session. `chat/turn/{n}` resolves turns in this session
    /// alone, so a hit from a rotated successor session has no turn coordinate.
    primary_session_id: Option<String>,
}

/// Where one transcript event sits in its conversation.
#[derive(Debug, Clone, Default)]
struct EventLocation {
    session_id: Option<String>,
    /// `turns.sequence` — the number `chat/turn/{n}` addresses.
    turn_seq: Option<i32>,
}

/// One transcript hit with the conversation coordinates the merge needs.
struct EventHit {
    hit: SearchIndexHit,
    turn_seq: Option<i32>,
    /// Whether `turn_seq` is addressable: it exists AND the event sits in the
    /// job's primary session.
    addressable: bool,
}

/// Build a URI for navigation from a hit's content type and coordinates.
///
/// `turn_seq` promotes a transcript URI from the whole conversation to the one
/// turn the excerpt came from; it is `None` whenever the turn is not
/// addressable.
fn build_uri(
    project_key: &str,
    content_type: &SearchContentType,
    nav: Option<&JobNav>,
    issue_number: Option<i32>,
    turn_seq: Option<i32>,
) -> String {
    let issue_or_project = || {
        issue_number
            .map(|number| build_issue_uri(project_key, number))
            .unwrap_or_else(|| build_project_uri(project_key))
    };

    match content_type {
        SearchContentType::Issue | SearchContentType::Comment => issue_or_project(),
        SearchContentType::Post | SearchContentType::PostComment => "cairn://posts".to_string(),
        SearchContentType::Message => issue_number
            .map(|number| build_issue_messages_uri(project_key, number))
            .unwrap_or_else(|| build_project_messages_uri(project_key)),
        SearchContentType::Artifact => match node_coordinate(nav, issue_number) {
            Some((number, node, exec)) => match task_segment(nav) {
                Some(task) => build_task_artifact_uri(project_key, number, exec, node, task),
                None => build_node_artifact_uri(project_key, number, exec, node),
            },
            None => issue_or_project(),
        },
        SearchContentType::Event => match node_coordinate(nav, issue_number) {
            Some((number, node, exec)) => match (task_segment(nav), turn_seq) {
                (Some(task), Some(turn)) => {
                    build_task_chat_turn_uri(project_key, number, exec, node, task, turn)
                }
                (Some(task), None) => build_task_chat_uri(project_key, number, exec, node, task),
                (None, Some(turn)) => {
                    build_node_chat_turn_uri(project_key, number, exec, node, turn)
                }
                (None, None) => build_node_chat_uri(project_key, number, exec, node),
            },
            None => issue_or_project(),
        },
    }
}

/// The (issue number, node segment, execution sequence) triple a node-scoped URI
/// needs. A thread is addressed by name at the zero coordinate, which the URI
/// builders recognize. `None` when any leg is missing — an unresolvable
/// coordinate must fall back to the issue rather than render a URI that does not
/// resolve.
fn node_coordinate(nav: Option<&JobNav>, issue_number: Option<i32>) -> Option<(i32, &str, i32)> {
    let nav = nav?;
    if let Some(thread_name) = nav.thread_name.as_deref() {
        return Some((0, thread_name, 0));
    }
    Some((issue_number?, nav.node_segment.as_deref()?, nav.exec_seq?))
}

fn task_segment(nav: Option<&JobNav>) -> Option<&str> {
    nav.and_then(|nav| nav.task_segment.as_deref())
}

/// Search content, fusing the text index with the semantic lane when one is
/// supplied and can answer.
pub async fn search_content(
    db: &LocalDb,
    index: &SearchIndex,
    query: &str,
    filters: Option<SearchFilters>,
    semantic: Option<SemanticLane<'_>>,
) -> Result<Vec<SearchResult>, String> {
    index
        .apply_pending(db)
        .await
        .map_err(|error| format!("Search index update failed: {error}"))?;

    let filters = filters.unwrap_or_default();
    let limit = filters.limit.unwrap_or(50).min(MAX_INDEX_LIMIT);
    let depth = candidate_limit(&filters, limit);
    let candidates = SearchFilters {
        limit: Some(depth),
        ..filters.clone()
    };

    let hits = index
        .search(query, Some(candidates))
        .map_err(|error| format!("Search failed: {error}"))?;

    // The semantic lane runs only when the text index found SOMETHING, and
    // only when transcripts are in scope at all.
    //
    // The first condition is the lane's relevance guard, and it lives here
    // because this is where the evidence is. A dense retriever cannot tell
    // whether it found anything — it always returns nearest neighbors, and
    // that geometry looks identical for a real question and for keyboard mash
    // (both measured; see `embeddings::turns`). But the text index CAN tell:
    // with scored disjunction any one token suffices, so a natural-language
    // question about work that happened matches something, while a nonsense
    // string or an identifier that appears nowhere matches nothing. Silence is
    // then the honest answer, and it is the answer search already gave.
    let vector_hits = match semantic {
        Some(lane) if semantic_applies(&filters) && !hits.is_empty() => {
            semantic_hits(db, &lane, query, &filters, depth).await
        }
        _ => Vec::new(),
    };

    let enrichment = Enrichment::load(db, &hits, &vector_hits).await?;
    let mut text_lane = enrichment.rank(hits);
    if vector_hits.is_empty() {
        // Nothing to fuse. Returning the text lane untouched — rather than
        // running a degenerate one-list fusion — is what makes the offline case
        // byte-identical to full-text search rather than merely similar.
        text_lane.truncate(limit);
        return Ok(text_lane);
    }
    Ok(fuse(text_lane, enrichment.rank(vector_hits), limit))
}

/// The semantic lane's contribution: transcript hits retrieved by vector
/// similarity, in the shape the text index produces.
///
/// Every failure is silent and total. An empty result means search answers from
/// the text index alone, which is the lane's whole contract.
async fn semantic_hits(
    db: &LocalDb,
    lane: &SemanticLane<'_>,
    query: &str,
    filters: &SearchFilters,
    limit: usize,
) -> Vec<SearchIndexHit> {
    let Some(scored) = lane
        .search
        .rank_turns(lane.vectors, filters.project_id.as_deref(), query, limit)
        .await
    else {
        return Vec::new();
    };

    let turn_ids: Vec<String> = scored.iter().map(|turn| turn.turn_id.clone()).collect();
    let coordinates = match load_turn_coordinates(db, &turn_ids).await {
        Ok(coordinates) => coordinates,
        Err(error) => {
            log::debug!("semantic search: resolving turn coordinates failed: {error}");
            return Vec::new();
        }
    };
    let excerpts = turn_excerpts(db, &turn_ids, EXCERPT_CHARS).await;

    scored
        .into_iter()
        .filter_map(|turn| {
            let coordinate = coordinates.get(&turn.turn_id)?;
            // Filters the text query pushes into Tantivy have to hold here too,
            // or narrowing a search would widen it.
            if filters
                .issue_id
                .as_ref()
                .is_some_and(|issue| Some(issue) != coordinate.issue_id.as_ref())
            {
                return None;
            }
            if filters
                .since
                .is_some_and(|since| coordinate.created_at < since)
            {
                return None;
            }
            let excerpt = excerpts.get(&turn.turn_id)?;
            Some(SearchIndexHit::transcript(
                excerpt.event_id.clone(),
                coordinate.project_id.clone(),
                coordinate.issue_id.clone(),
                coordinate.job_id.clone(),
                excerpt.event_type.clone(),
                excerpt.text.clone(),
                turn.similarity as f64,
                coordinate.created_at,
            ))
        })
        .collect()
}

/// Fuse two independently ranked lists of places by reciprocal rank.
///
/// The lanes score in incommensurable units — a Tantivy relevance score and a
/// cosine similarity — so their VALUES cannot be combined. Their POSITIONS can:
/// each row takes `1/(damping + position)` from every lane that ranked it, and
/// a place both lanes rank highly beats one that only a single lane found.
///
/// A place found by both keeps the text lane's row wholesale — its excerpt is
/// the passage that literally matched, and its `hit_count` counts text matches.
/// Only the ordering changes. Widening the row to cover the semantic span's
/// turns would imply matches across a range the count does not support.
fn fuse(
    text_lane: Vec<SearchResult>,
    vector_lane: Vec<SearchResult>,
    limit: usize,
) -> Vec<SearchResult> {
    let reciprocal = |position: usize| 1.0 / (RRF_DAMPING + position as f64 + 1.0);

    let mut fused: Vec<(f64, SearchResult)> = text_lane
        .into_iter()
        .enumerate()
        .map(|(position, result)| (reciprocal(position), result))
        .collect();

    for (position, result) in vector_lane.into_iter().enumerate() {
        let score = reciprocal(position);
        match fused
            .iter_mut()
            .find(|(_, existing)| same_place(existing, &result))
        {
            Some((existing_score, _)) => *existing_score += score,
            None => fused.push((score, result)),
        }
    }

    fused.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.1.created_at.cmp(&a.1.created_at))
            .then(a.1.id.cmp(&b.1.id))
    });
    let mut results: Vec<SearchResult> = fused
        .into_iter()
        .map(|(score, mut result)| {
            // Keep `rank` agreeing with the order actually returned, so a
            // consumer that re-sorts by rank cannot disagree with the list.
            result.rank = score;
            result
        })
        .collect();
    results.truncate(limit);
    results
}

/// Whether two transcript rows name the same stretch of conversation.
///
/// Both lanes merge with the same turn-distance rule, but they merge different
/// hits, so their spans can cover overlapping rather than identical ranges.
/// Two spans in one job are the same place when their ranges touch within the
/// same gap that built them. A span with no addressable turn IS its whole
/// conversation, so there the URI is the identity.
fn same_place(a: &SearchResult, b: &SearchResult) -> bool {
    if a.content_type != SearchContentType::Event || b.content_type != SearchContentType::Event {
        return false;
    }
    if a.job_id.is_none() || a.job_id != b.job_id {
        return false;
    }
    match (a.turn_start, a.turn_end, b.turn_start, b.turn_end) {
        (Some(a_start), Some(a_end), Some(b_start), Some(b_end)) => {
            b_start <= a_end + SPAN_TURN_GAP && a_start <= b_end + SPAN_TURN_GAP
        }
        _ => a.uri == b.uri,
    }
}

/// How many documents to pull from the index. Only a query that can return
/// transcript hits over-fetches; everything else maps one hit to one row and
/// would pay the extra enrichment for nothing.
fn candidate_limit(filters: &SearchFilters, limit: usize) -> usize {
    if !includes_events(filters) {
        return limit;
    }
    limit
        .saturating_mul(SPAN_CANDIDATE_FACTOR)
        .clamp(limit, MAX_INDEX_LIMIT)
}

/// Whether the semantic lane can honor this search at all.
///
/// A filter the text lane pushes into Tantivy has to hold for this lane too, or
/// narrowing a search would widen it. Two of them cannot be expressed against a
/// retriever that works on whole turns, so the lane stands down rather than
/// answering a different question:
///
/// - `role` selects an author. A turn has no author — it is an exchange — and
///   the excerpt a semantic hit renders is the turn's LONGEST content event,
///   which is nearly always the assistant's. Filtering that after the fact
///   would filter the rendering, not the retrieval, so `role=user` would answer
///   with assistant messages.
/// - `in=title` restricts matching to the title field. This lane only ever
///   matches bodies, so it would inject body matches into a title-only search.
///
/// `issue` and `since` DO carry over, and are applied in [`semantic_hits`].
fn semantic_applies(filters: &SearchFilters) -> bool {
    includes_events(filters) && filters.role.is_none() && !filters.title_only
}

fn includes_events(filters: &SearchFilters) -> bool {
    filters.content_types.as_ref().is_none_or(|content_types| {
        content_types
            .iter()
            .any(|content_type| content_type == "event")
    })
}

/// Everything the row builders need to turn index hits into navigable results.
struct Enrichment {
    project_keys: HashMap<String, String>,
    issue_info: HashMap<String, (i32, String)>,
    job_nav: HashMap<String, JobNav>,
    event_locations: HashMap<String, EventLocation>,
}

impl Enrichment {
    /// Resolve the coordinates every lane's hits will need, in one pass over
    /// both. One enrichment serves both lanes so a place that appears in each
    /// is described identically — the precondition for recognizing it as one
    /// place during fusion.
    async fn load(
        db: &LocalDb,
        text_hits: &[SearchIndexHit],
        vector_hits: &[SearchIndexHit],
    ) -> Result<Self, String> {
        let all = || text_hits.iter().chain(vector_hits.iter());
        Ok(Enrichment {
            project_keys: load_project_keys(db, unique(all().map(|hit| Some(&hit.project_id))))
                .await?,
            issue_info: load_issue_info(db, unique(all().map(|hit| hit.issue_id.as_ref()))).await?,
            job_nav: load_job_nav(db, unique(all().map(|hit| hit.job_id.as_ref()))).await?,
            event_locations: load_event_locations(
                db,
                unique(
                    all()
                        .filter(|hit| hit.content_type == SearchContentType::Event)
                        .map(|hit| Some(&hit.id)),
                ),
            )
            .await?,
        })
    }

    /// One lane's hits as a ranked list of places, best first.
    ///
    /// Untruncated: fusion needs each lane's full ordering, and a caller
    /// serving a single lane truncates itself.
    fn rank(&self, hits: Vec<SearchIndexHit>) -> Vec<SearchResult> {
        let (event_hits, other_hits): (Vec<SearchIndexHit>, Vec<SearchIndexHit>) = hits
            .into_iter()
            .partition(|hit| hit.content_type == SearchContentType::Event);

        let mut results: Vec<SearchResult> = other_hits
            .into_iter()
            .filter_map(|hit| self.single_result(hit))
            .collect();
        results.extend(self.hotspot_results(event_hits));

        // Rank descending, then newest, then id: a total order, so equal-ranked
        // rows do not shuffle between identical queries.
        results.sort_by(|a, b| {
            b.rank
                .partial_cmp(&a.rank)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(b.created_at.cmp(&a.created_at))
                .then(a.id.cmp(&b.id))
        });
        results
    }

    /// Issue number and title context for a hit, as the result rows carry it.
    fn issue_context(&self, hit: &SearchIndexHit) -> (Option<i32>, Option<String>) {
        hit.issue_id
            .as_ref()
            .and_then(|id| self.issue_info.get(id))
            .map(|(number, title)| (Some(*number), Some(title.clone())))
            .unwrap_or((None, None))
    }

    /// One hit, one row — the shape every non-transcript content type keeps.
    fn single_result(&self, hit: SearchIndexHit) -> Option<SearchResult> {
        // Posts are global resources even when their visibility is narrowed to a
        // project. Workspace posts intentionally have no project id, so requiring
        // project enrichment here would silently discard the default post scope.
        let project_key = if matches!(
            hit.content_type,
            SearchContentType::Post | SearchContentType::PostComment
        ) {
            None
        } else {
            Some(self.project_keys.get(&hit.project_id)?.clone())
        };
        let (issue_number, issue_title) = self.issue_context(&hit);
        let nav = hit.job_id.as_ref().and_then(|id| self.job_nav.get(id));
        let uri = match hit.content_type {
            SearchContentType::Post => format!("cairn://posts/{}", hit.id),
            SearchContentType::PostComment => "cairn://posts".to_string(),
            _ => build_uri(
                project_key
                    .as_deref()
                    .expect("non-post project was enriched"),
                &hit.content_type,
                nav,
                issue_number,
                None,
            ),
        };

        // An issue hit IS the issue; repeating its own number and title as
        // "context" would say nothing.
        let (context_number, context_title) = if hit.content_type == SearchContentType::Issue {
            (None, None)
        } else {
            (issue_number, issue_title)
        };

        Some(SearchResult {
            id: hit.id,
            content_type: hit.content_type,
            project_id: hit.project_id,
            issue_id: hit.issue_id,
            job_id: hit.job_id,
            title: hit.title,
            snippet: hit.snippet,
            rank: hit.rank,
            created_at: hit.created_at,
            uri,
            issue_number: context_number,
            issue_title: context_title,
            node_segment: nav.and_then(|nav| nav.node_segment.clone()),
            task_segment: nav.and_then(|nav| nav.task_segment.clone()),
            exec_seq: nav.and_then(|nav| nav.exec_seq),
            hit_count: 1,
            turn_start: None,
            turn_end: None,
        })
    }

    /// Merge transcript hits into one row per stretch of conversation.
    ///
    /// Hits group by (job, session) — one continuous transcript — and then split
    /// on turn distance, so two discussions far apart in the same node stay two
    /// hotspots. A hit whose job or session cannot be resolved has no
    /// conversation to belong to and stays a row of its own.
    fn hotspot_results(&self, hits: Vec<SearchIndexHit>) -> Vec<SearchResult> {
        let mut conversations: HashMap<(String, String), Vec<EventHit>> = HashMap::new();
        let mut loose: Vec<EventHit> = Vec::new();

        for hit in hits {
            let location = self
                .event_locations
                .get(&hit.id)
                .cloned()
                .unwrap_or_default();
            let nav = hit.job_id.as_ref().and_then(|id| self.job_nav.get(id));
            let addressable = location.turn_seq.is_some()
                && nav.is_some_and(|nav| nav.primary_session_id == location.session_id);
            let event = EventHit {
                hit,
                turn_seq: location.turn_seq,
                addressable,
            };
            match (event.hit.job_id.clone(), location.session_id) {
                (Some(job_id), Some(session_id)) => conversations
                    .entry((job_id, session_id))
                    .or_default()
                    .push(event),
                _ => loose.push(event),
            }
        }

        let mut results: Vec<SearchResult> = loose
            .into_iter()
            .filter_map(|event| self.span_result(vec![event]))
            .collect();

        for (_, mut events) in conversations {
            // Turnless hits (legacy transcripts) cannot be ordered against
            // turned ones; they collapse into one span for the conversation.
            let turnless: Vec<EventHit> =
                drain_where(&mut events, |event| event.turn_seq.is_none());
            if !turnless.is_empty() {
                results.extend(self.span_result(turnless));
            }

            events.sort_by_key(|event| event.turn_seq.unwrap_or_default());
            let turns: Vec<(usize, i32)> = events
                .iter()
                .enumerate()
                .filter_map(|(index, event)| event.turn_seq.map(|turn| (index, turn)))
                .collect();
            let mut spans = merge_adjacent_turns(&turns);
            // Drain by descending index so each removal leaves earlier ones put.
            spans.sort_by_key(|span| std::cmp::Reverse(span.first().copied().unwrap_or_default()));
            for span in spans {
                let members: Vec<EventHit> = span
                    .into_iter()
                    .rev()
                    .map(|index| events.remove(index))
                    .collect();
                results.extend(self.span_result(members));
            }
        }

        results
    }

    /// Render one span as a single hotspot row: the peak hit's excerpt, the
    /// number of matches merged, and the turn range the stretch covers.
    fn span_result(&self, mut members: Vec<EventHit>) -> Option<SearchResult> {
        let peak_index = members
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.hit
                    .rank
                    .partial_cmp(&b.hit.rank)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(index, _)| index)?;

        let hit_count = members.len();
        let accrued: f64 = members
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != peak_index)
            .map(|(_, member)| member.hit.rank)
            .sum();
        let addressable = members.iter().all(|member| member.addressable);
        let turn_bounds = addressable
            .then(|| {
                let turns: Vec<i32> = members
                    .iter()
                    .filter_map(|member| member.turn_seq)
                    .collect();
                turns.iter().min().copied().zip(turns.iter().max().copied())
            })
            .flatten();

        let peak = members.swap_remove(peak_index);
        let hit = peak.hit;
        let project_key = self.project_keys.get(&hit.project_id)?.clone();
        let (issue_number, issue_title) = self.issue_context(&hit);
        let nav = hit.job_id.as_ref().and_then(|id| self.job_nav.get(id));
        let turn_seq = peak.addressable.then_some(peak.turn_seq).flatten();
        let uri = build_uri(
            &project_key,
            &SearchContentType::Event,
            nav,
            issue_number,
            turn_seq,
        );

        Some(SearchResult {
            // The conversation's label, not the peak event's type: a hotspot is
            // a place in a node's transcript, and `assistant` names neither.
            title: span_title(nav, &hit.title),
            id: hit.id,
            content_type: SearchContentType::Event,
            project_id: hit.project_id,
            issue_id: hit.issue_id,
            job_id: hit.job_id,
            snippet: hit.snippet,
            rank: hit.rank + SPAN_ACCRUAL * accrued,
            created_at: hit.created_at,
            uri,
            issue_number,
            issue_title,
            node_segment: nav.and_then(|nav| nav.node_segment.clone()),
            task_segment: nav.and_then(|nav| nav.task_segment.clone()),
            exec_seq: nav.and_then(|nav| nav.exec_seq),
            hit_count,
            turn_start: turn_bounds.map(|(start, _)| start),
            turn_end: turn_bounds.map(|(_, end)| end),
        })
    }
}

/// Label a transcript hotspot by the conversation it lives in: the task segment
/// when the hit is a sub-agent's, else the node segment, falling back to the
/// event type when the job never resolved.
fn span_title(nav: Option<&JobNav>, event_type: &str) -> String {
    nav.and_then(|nav| {
        nav.task_segment
            .clone()
            .or_else(|| nav.node_segment.clone())
    })
    .unwrap_or_else(|| event_type.to_string())
}

/// Split turn-ordered hits into spans of adjacent turns.
///
/// `turns` is `(index, turn sequence)` sorted ascending by turn. A gap wider
/// than [`SPAN_TURN_GAP`] starts a new span: two discussions in the same node
/// are two places, not one.
fn merge_adjacent_turns(turns: &[(usize, i32)]) -> Vec<Vec<usize>> {
    let mut spans: Vec<Vec<usize>> = Vec::new();
    let mut previous: Option<i32> = None;
    for (index, turn) in turns {
        match previous {
            Some(last) if turn - last <= SPAN_TURN_GAP => {
                spans
                    .last_mut()
                    .expect("previous turn opened a span")
                    .push(*index);
            }
            _ => spans.push(vec![*index]),
        }
        previous = Some(*turn);
    }
    spans
}

/// Remove and return every element matching `predicate`, preserving the order of
/// both the removed and the retained elements.
fn drain_where<T>(items: &mut Vec<T>, predicate: impl Fn(&T) -> bool) -> Vec<T> {
    let mut removed = Vec::new();
    let mut index = 0;
    while index < items.len() {
        if predicate(&items[index]) {
            removed.push(items.remove(index));
        } else {
            index += 1;
        }
    }
    removed
}

/// Sorted, deduplicated ids from an iterator of optional references.
fn unique<'a>(values: impl Iterator<Item = Option<&'a String>>) -> Vec<String> {
    let mut ids: Vec<String> = values.flatten().cloned().collect();
    ids.sort();
    ids.dedup();
    ids
}

async fn load_project_keys(
    db: &LocalDb,
    project_ids: Vec<String>,
) -> Result<HashMap<String, String>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut map = HashMap::new();
            for project_id in project_ids {
                let mut rows = conn
                    .query(
                        "SELECT key FROM projects WHERE id = ?1",
                        (project_id.as_str(),),
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    map.insert(project_id, row.text(0)?);
                }
            }
            Ok(map)
        })
    })
    .await
    .map_err(storage_error)
}

async fn load_issue_info(
    db: &LocalDb,
    issue_ids: Vec<String>,
) -> Result<HashMap<String, (i32, String)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut map = HashMap::new();
            for issue_id in issue_ids {
                let mut rows = conn
                    .query(
                        "SELECT number, title FROM issues WHERE id = ?1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    map.insert(issue_id, (row.i64(0)? as i32, row.text(1)?));
                }
            }
            Ok(map)
        })
    })
    .await
    .map_err(storage_error)
}

/// Load the addressable coordinates for each hit's job.
///
/// A sub-agent task job is addressed through its parent node, so the node
/// segment and execution sequence come from the parent whenever there is one.
/// The primary session is the first run's — the only session whose turns the
/// `chat/turn/{n}` coordinate resolves.
async fn load_job_nav(
    db: &LocalDb,
    job_ids: Vec<String>,
) -> Result<HashMap<String, JobNav>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut map = HashMap::new();
            for job_id in job_ids {
                let mut rows = conn
                    .query(
                        "SELECT COALESCE(parent.uri_segment, job.uri_segment),
                                CASE WHEN job.parent_job_id IS NULL THEN NULL ELSE job.uri_segment END,
                                COALESCE(exec.seq, parent_exec.seq),
                                (SELECT run.session_id
                                   FROM runs run
                                  WHERE run.job_id = job.id
                                  ORDER BY run.created_at ASC
                                  LIMIT 1),
                                thread.name
                           FROM jobs job
                           LEFT JOIN jobs parent ON parent.id = job.parent_job_id
                           LEFT JOIN executions exec ON exec.id = job.execution_id
                           LEFT JOIN executions parent_exec ON parent_exec.id = parent.execution_id
                           LEFT JOIN threads thread
                                  ON thread.id = COALESCE(job.thread_id, parent.thread_id)
                          WHERE job.id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    let thread_name = row.opt_text(4)?;
                    map.insert(
                        job_id,
                        JobNav {
                            // A thread's own segment reads `thread`; the name is
                            // what addresses it, so report that as the segment.
                            node_segment: thread_name.clone().or(row.opt_text(0)?),
                            task_segment: row.opt_text(1)?,
                            thread_name,
                            exec_seq: row.opt_i64(2)?.map(|seq| seq as i32),
                            primary_session_id: row.opt_text(3)?,
                        },
                    );
                }
            }
            Ok(map)
        })
    })
    .await
    .map_err(storage_error)
}

/// Load each transcript hit's place in its conversation, in one query. The turn
/// row is authoritative for the session: an event carries its own `session_id`,
/// but the turn is what the `chat/turn/{n}` coordinate is keyed by.
async fn load_event_locations(
    db: &LocalDb,
    event_ids: Vec<String>,
) -> Result<HashMap<String, EventLocation>, String> {
    if event_ids.is_empty() {
        return Ok(HashMap::new());
    }
    db.read(|conn| {
        let event_ids = event_ids.clone();
        Box::pin(async move {
            let placeholders = (1..=event_ids.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT event.id, COALESCE(turn.session_id, event.session_id), turn.sequence
                   FROM events event
                   LEFT JOIN turns turn ON turn.id = event.turn_id
                  WHERE event.id IN ({placeholders})"
            );
            let params: Vec<cairn_db::turso::Value> = event_ids
                .iter()
                .map(|id| cairn_db::turso::Value::Text(id.clone()))
                .collect();

            let mut map = HashMap::new();
            let mut rows = conn.query(&sql, params).await?;
            while let Some(row) = rows.next().await? {
                map.insert(
                    row.text(0)?,
                    EventLocation {
                        session_id: row.opt_text(1)?,
                        turn_seq: row.opt_i64(2)?.map(|seq| seq as i32),
                    },
                );
            }
            Ok(map)
        })
    })
    .await
    .map_err(storage_error)
}

/// Where one turn sits in the project graph, for a semantic hit that starts
/// from a turn rather than from an indexed document.
#[derive(Debug, Clone)]
struct TurnCoordinate {
    project_id: String,
    issue_id: Option<String>,
    job_id: Option<String>,
    created_at: i64,
}

/// Resolve the owning project, issue, and job of each retrieved turn.
///
/// A turn whose run carries no project cannot be addressed and is simply
/// absent from the map, which drops it from the lane.
async fn load_turn_coordinates(
    db: &LocalDb,
    turn_ids: &[String],
) -> Result<HashMap<String, TurnCoordinate>, String> {
    if turn_ids.is_empty() {
        return Ok(HashMap::new());
    }
    db.read(|conn| {
        let turn_ids = turn_ids.to_vec();
        Box::pin(async move {
            let placeholders = (1..=turn_ids.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT turn.id, run.project_id, run.issue_id,
                        COALESCE(turn.job_id, run.job_id), turn.created_at
                   FROM turns turn
                   JOIN runs run ON run.id = turn.run_id
                  WHERE turn.id IN ({placeholders}) AND run.project_id IS NOT NULL"
            );
            let params: Vec<cairn_db::turso::Value> = turn_ids
                .iter()
                .map(|id| cairn_db::turso::Value::Text(id.clone()))
                .collect();

            let mut map = HashMap::new();
            let mut rows = conn.query(&sql, params).await?;
            while let Some(row) = rows.next().await? {
                map.insert(
                    row.text(0)?,
                    TurnCoordinate {
                        project_id: row.text(1)?,
                        issue_id: row.opt_text(2)?,
                        job_id: row.opt_text(3)?,
                        created_at: row.i64(4)?,
                    },
                );
            }
            Ok(map)
        })
    })
    .await
    .map_err(storage_error)
}

fn storage_error(error: DbError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, SearchIndex};
    use tempfile::tempdir;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("cairn-search-content-turso.db").await
    }

    fn node_nav(segment: &str, exec_seq: i32) -> JobNav {
        JobNav {
            node_segment: Some(segment.to_string()),
            exec_seq: Some(exec_seq),
            ..Default::default()
        }
    }

    #[test]
    fn test_build_uri_issue() {
        let uri = build_uri("test", &SearchContentType::Issue, None, Some(42), None);
        assert_eq!(uri, "cairn://p/test/42");
    }

    #[test]
    fn test_build_uri_comment() {
        let uri = build_uri("test", &SearchContentType::Comment, None, Some(42), None);
        assert_eq!(uri, "cairn://p/test/42");
    }

    #[test]
    fn test_build_uri_message_uses_message_resources() {
        let project_uri = build_uri("test", &SearchContentType::Message, None, None, None);
        assert_eq!(project_uri, "cairn://p/test/messages");

        let issue_uri = build_uri("test", &SearchContentType::Message, None, Some(42), None);
        assert_eq!(issue_uri, "cairn://p/test/42/messages");
    }

    #[test]
    fn test_build_uri_artifact_prefers_node_artifact_when_job_navigation_exists() {
        let uri = build_uri(
            "test",
            &SearchContentType::Artifact,
            Some(&node_nav("builder-1", 3)),
            Some(42),
            None,
        );
        assert_eq!(uri, "cairn://p/test/42/3/builder-1/artifact");
    }

    #[test]
    fn test_build_uri_artifact_falls_back_when_job_navigation_missing() {
        let issue_uri = build_uri(
            "test",
            &SearchContentType::Artifact,
            Some(&JobNav {
                node_segment: Some("builder-1".to_string()),
                ..Default::default()
            }),
            Some(42),
            None,
        );
        assert_eq!(issue_uri, "cairn://p/test/42");

        let project_uri = build_uri("test", &SearchContentType::Artifact, None, None, None);
        assert_eq!(project_uri, "cairn://p/test");
    }

    #[test]
    fn test_build_uri_event_prefers_node_chat_when_job_navigation_exists() {
        let uri = build_uri(
            "test",
            &SearchContentType::Event,
            Some(&node_nav("builder-1", 3)),
            Some(42),
            None,
        );
        assert_eq!(uri, "cairn://p/test/42/3/builder-1/chat");
    }

    #[test]
    fn test_build_uri_event_addresses_the_turn_when_one_is_resolvable() {
        let uri = build_uri(
            "test",
            &SearchContentType::Event,
            Some(&node_nav("builder", 1)),
            Some(42),
            Some(7),
        );
        assert_eq!(uri, "cairn://p/test/42/1/builder/chat/turn/7");
    }

    #[test]
    fn test_build_uri_event_from_a_task_addresses_the_task_transcript() {
        let nav = JobNav {
            node_segment: Some("builder".to_string()),
            task_segment: Some("explore".to_string()),
            exec_seq: Some(1),
            ..Default::default()
        };
        assert_eq!(
            build_uri(
                "test",
                &SearchContentType::Event,
                Some(&nav),
                Some(42),
                Some(3)
            ),
            "cairn://p/test/42/1/builder/task/explore/chat/turn/3"
        );
        assert_eq!(
            build_uri(
                "test",
                &SearchContentType::Event,
                Some(&nav),
                Some(42),
                None
            ),
            "cairn://p/test/42/1/builder/task/explore/chat"
        );
    }

    #[test]
    fn test_build_uri_event_falls_back_when_job_navigation_missing() {
        let issue_uri = build_uri(
            "test",
            &SearchContentType::Event,
            Some(&JobNav {
                exec_seq: Some(3),
                ..Default::default()
            }),
            Some(42),
            None,
        );
        assert_eq!(issue_uri, "cairn://p/test/42");

        let project_uri = build_uri("test", &SearchContentType::Event, None, None, None);
        assert_eq!(project_uri, "cairn://p/test");
    }

    #[test]
    fn test_build_uri_event_from_a_thread_addresses_the_thread_by_name() {
        // A thread has neither issue number nor execution: its name IS the
        // coordinate, at the zero node address.
        let nav = JobNav {
            node_segment: Some("general".to_string()),
            thread_name: Some("general".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_uri("test", &SearchContentType::Event, Some(&nav), None, Some(3)),
            "cairn://p/test/general/chat/turn/3"
        );

        let task = JobNav {
            task_segment: Some("map-settings".to_string()),
            ..nav
        };
        assert_eq!(
            build_uri("test", &SearchContentType::Event, Some(&task), None, None),
            "cairn://p/test/general/task/map-settings/chat"
        );
    }

    #[test]
    fn adjacent_turns_merge_and_distant_turns_split() {
        // Turns 4 and 6 sit within the gap; 20 starts its own stretch.
        let spans = merge_adjacent_turns(&[(0, 4), (1, 6), (2, 20), (3, 21)]);
        assert_eq!(spans, vec![vec![0, 1], vec![2, 3]]);
    }

    #[test]
    fn a_lone_turn_is_its_own_span() {
        assert_eq!(merge_adjacent_turns(&[(7, 12)]), vec![vec![7]]);
        assert!(merge_adjacent_turns(&[]).is_empty());
    }

    #[test]
    fn candidate_limit_over_fetches_only_when_events_can_match() {
        let events_only = SearchFilters {
            content_types: Some(vec!["event".to_string()]),
            ..Default::default()
        };
        assert_eq!(candidate_limit(&events_only, 8), 32);
        assert_eq!(candidate_limit(&events_only, 50), MAX_INDEX_LIMIT);

        let no_events = SearchFilters {
            content_types: Some(vec!["issue".to_string(), "comment".to_string()]),
            ..Default::default()
        };
        assert_eq!(candidate_limit(&no_events, 8), 8);

        // Unfiltered searches can return transcript hits, so they over-fetch.
        assert_eq!(candidate_limit(&SearchFilters::default(), 10), 40);
    }

    #[tokio::test]
    async fn search_content_returns_existing_search_result_shape() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
             VALUES ('workspace-1', 'Workspace', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'workspace-1', 'Project', 'proj', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 7, 'Turso migration', 'issue body', 1, 1);
            INSERT INTO comments(id, issue_id, content, source, created_at)
             VALUES ('comment-1', 'issue-1', 'tantivy replacement comment', 'user', 2);
            ",
        )
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        let results = search_content(
            &db,
            &index,
            "tantivy",
            Some(SearchFilters {
                project_id: Some("project-1".to_string()),
                issue_id: Some("issue-1".to_string()),
                content_types: Some(vec!["comment".to_string()]),
                role: None,
                title_only: false,
                since: None,
                limit: Some(10),
            }),
            None,
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.id, "comment-1");
        assert_eq!(result.content_type, SearchContentType::Comment);
        assert_eq!(result.uri, "cairn://p/proj/7");
        assert_eq!(result.issue_number, Some(7));
        assert_eq!(result.issue_title.as_deref(), Some("Turso migration"));
        assert_eq!(result.hit_count, 1);
        assert_eq!(result.turn_start, None);
        assert!(result.snippet.contains("<mark>tantivy</mark>"));
    }

    // ===== fusion =====

    /// A transcript span row, as either lane produces one.
    fn span(id: &str, job: &str, turns: Option<(i32, i32)>, rank: f64) -> SearchResult {
        SearchResult {
            id: id.to_string(),
            content_type: SearchContentType::Event,
            project_id: "project-1".to_string(),
            issue_id: Some("issue-1".to_string()),
            job_id: Some(job.to_string()),
            title: "builder".to_string(),
            snippet: id.to_string(),
            rank,
            created_at: 100,
            uri: format!("cairn://p/proj/12/1/{job}/chat"),
            issue_number: Some(12),
            issue_title: None,
            node_segment: Some(job.to_string()),
            task_segment: None,
            exec_seq: Some(1),
            hit_count: 1,
            turn_start: turns.map(|(start, _)| start),
            turn_end: turns.map(|(_, end)| end),
        }
    }

    #[test]
    fn overlapping_spans_in_one_job_are_the_same_place() {
        // The lanes merge different hits, so their spans cover overlapping
        // rather than identical ranges. Touching within the merge gap is the
        // same rule that built each span.
        assert!(same_place(
            &span("a", "job-1", Some((10, 12)), 1.0),
            &span("b", "job-1", Some((11, 14)), 1.0)
        ));
        assert!(same_place(
            &span("a", "job-1", Some((10, 12)), 1.0),
            &span("b", "job-1", Some((14, 15)), 1.0)
        ));
        // Far apart in the same node: two discussions, two places.
        assert!(!same_place(
            &span("a", "job-1", Some((10, 12)), 1.0),
            &span("b", "job-1", Some((40, 41)), 1.0)
        ));
        // Same turns, different conversations.
        assert!(!same_place(
            &span("a", "job-1", Some((10, 12)), 1.0),
            &span("b", "job-2", Some((10, 12)), 1.0)
        ));
    }

    #[test]
    fn a_place_both_lanes_found_outranks_one_either_found_alone() {
        // The text lane's own order puts `text-top` first, but the semantic
        // lane also found `agreed` — which is exactly the signal fusion exists
        // to reward.
        let text = vec![
            span("text-top", "job-1", Some((5, 5)), 9.0),
            span("agreed", "job-2", Some((20, 21)), 1.0),
        ];
        let vector = vec![span("agreed-vec", "job-2", Some((21, 22)), 0.8)];

        let fused = fuse(text, vector, 10);
        assert_eq!(fused.len(), 2);
        assert_eq!(fused[0].id, "agreed");
        // The text row survives wholesale: its excerpt is the passage that
        // literally matched.
        assert_eq!(fused[0].snippet, "agreed");
        assert!(fused[0].rank > fused[1].rank);
    }

    #[test]
    fn a_place_only_the_semantic_lane_found_still_enters() {
        // The paraphrase case: the conversation uses none of the query's words,
        // so the text lane cannot reach it at all.
        let text = vec![span("text-only", "job-1", Some((5, 5)), 9.0)];
        let vector = vec![span("vector-only", "job-9", Some((3, 4)), 0.7)];

        let fused = fuse(text, vector, 10);
        let mut ids: Vec<&str> = fused.iter().map(|result| result.id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["text-only", "vector-only"]);
    }

    #[test]
    fn the_semantic_lane_can_outrank_a_weak_text_hit() {
        // Scored disjunction lets a hit through on one common word, so the
        // text lane's tail is not evidence of much. A strong semantic hit has
        // to be able to climb past it, or a paraphrase query is answered by
        // whatever happened to contain "the".
        let text: Vec<SearchResult> = (0..20)
            .map(|index| {
                span(
                    &format!("stopword-{index:02}"),
                    &format!("job-{index}"),
                    Some((1, 1)),
                    1.0,
                )
            })
            .collect();
        let vector = vec![span("on-topic", "job-99", Some((3, 4)), 0.4)];

        let fused = fuse(text, vector, 10);
        let position = fused
            .iter()
            .position(|result| result.id == "on-topic")
            .expect("the semantic hit is in the answer");
        assert!(position < 3, "semantic hit landed at position {position}");
    }

    #[tokio::test]
    async fn a_query_the_text_index_cannot_match_stays_empty() {
        // The semantic lane's relevance guard. Cosine would happily return the
        // least-unrelated conversations for this; the text index finding
        // nothing is what says there is nothing to find.
        let db = migrated_db().await;
        seed_transcript(&db).await;
        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();

        let results = search_content(&db, &index, "asdfjkl qwertyuiop", None, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn fusion_ranks_agree_with_the_order_returned() {
        let text = vec![
            span("a", "job-1", Some((1, 1)), 9.0),
            span("b", "job-2", Some((1, 1)), 8.0),
        ];
        let vector = vec![span("c", "job-3", Some((1, 1)), 0.9)];

        let fused = fuse(text, vector, 10);
        // A consumer re-sorting by rank must not disagree with the list order.
        for pair in fused.windows(2) {
            assert!(pair[0].rank >= pair[1].rank);
        }
    }

    #[test]
    fn filters_the_semantic_lane_cannot_express_stand_it_down() {
        let base = SearchFilters {
            limit: Some(10),
            ..Default::default()
        };
        assert!(semantic_applies(&base));

        // A turn has no author, and the excerpt is whichever event is longest,
        // so honoring `role` would mean filtering the rendering.
        assert!(!semantic_applies(&SearchFilters {
            role: Some("user".to_string()),
            ..base.clone()
        }));
        // A body retriever has nothing to say about a title-only search.
        assert!(!semantic_applies(&SearchFilters {
            title_only: true,
            ..base.clone()
        }));
        // Transcripts excluded entirely: nothing for this lane to contribute.
        assert!(!semantic_applies(&SearchFilters {
            content_types: Some(vec!["issue".to_string()]),
            ..base.clone()
        }));
        // `issue` and `since` DO carry over, so they must not stand it down.
        assert!(semantic_applies(&SearchFilters {
            issue_id: Some("issue-1".to_string()),
            since: Some(1),
            ..base
        }));
    }

    #[test]
    fn a_non_transcript_row_never_merges_with_a_span() {
        let mut issue = span("issue-row", "job-1", Some((1, 1)), 5.0);
        issue.content_type = SearchContentType::Issue;
        assert!(!same_place(
            &issue,
            &span("span", "job-1", Some((1, 1)), 1.0)
        ));
    }

    /// One node, one session, two turns of conversation, plus a much later turn
    /// in the same node and a second node that only mentions one of the words.
    async fn seed_transcript(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
             VALUES ('workspace-1', 'Workspace', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'workspace-1', 'Project', 'proj', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 12, 'Search hotspots', 'issue body', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'issue-1', 'project-1', 'running', 'Builder', 'builder', 1, 1);
            INSERT INTO runs(id, issue_id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-1', 'issue-1', 'project-1', 'job-1', 'exited', 'session-1', 1, 1);
            INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
             VALUES ('turn-4', 'session-1', 'run-1', 'job-1', 4, 'done', 1, 1),
                    ('turn-5', 'session-1', 'run-1', 'job-1', 5, 'done', 2, 2),
                    ('turn-40', 'session-1', 'run-1', 'job-1', 40, 'done', 3, 3);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES ('ev-1', 'run-1', 'session-1', 1, 1, 'user',
                     '{\"content\":\"what about zephyrine\"}', 1, 'turn-4'),
                    ('ev-2', 'run-1', 'session-1', 2, 2, 'assistant',
                     '{\"content\":\"quixotry is the answer\"}', 2, 'turn-5'),
                    ('ev-3', 'run-1', 'session-1', 3, 3, 'assistant',
                     '{\"content\":\"zephyrine came up again much later\"}', 3, 'turn-40');
            ",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn adjacent_event_hits_merge_into_one_hotspot_with_a_turn_link() {
        let db = migrated_db().await;
        seed_transcript(&db).await;

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();

        // Neither word appears in a single event: "zephyrine" is in turn 4 and
        // "quixotry" in turn 5. The stretch is one place, and the far-off turn
        // 40 mention is another.
        let results = search_content(&db, &index, "zephyrine quixotry", None, None)
            .await
            .unwrap();

        assert_eq!(results.len(), 2, "two stretches, not three events");

        let hotspot = &results[0];
        assert_eq!(hotspot.hit_count, 2);
        assert_eq!(hotspot.turn_start, Some(4));
        assert_eq!(hotspot.turn_end, Some(5));
        assert_eq!(hotspot.title, "builder");
        assert_eq!(hotspot.issue_number, Some(12));
        assert!(
            hotspot
                .uri
                .starts_with("cairn://p/proj/12/1/builder/chat/turn/"),
            "expected a turn link, got {}",
            hotspot.uri
        );

        let distant = &results[1];
        assert_eq!(distant.hit_count, 1);
        assert_eq!(distant.turn_start, Some(40));
        assert_eq!(distant.uri, "cairn://p/proj/12/1/builder/chat/turn/40");
        assert!(
            hotspot.rank > distant.rank,
            "the denser stretch must outrank the lone mention"
        );
    }

    #[tokio::test]
    async fn a_hit_outside_the_primary_session_keeps_the_conversation_uri() {
        let db = migrated_db().await;
        seed_transcript(&db).await;
        // A rotated successor session: its turns are real, but `chat/turn/{n}`
        // only resolves turns of the job's FIRST session, so no turn link.
        db.execute_script(
            "
            INSERT INTO runs(id, issue_id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-2', 'issue-1', 'project-1', 'job-1', 'exited', 'session-2', 9, 9);
            INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
             VALUES ('turn-b1', 'session-2', 'run-2', 'job-1', 1, 'done', 9, 9);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES ('ev-9', 'run-2', 'session-2', 1, 9, 'assistant',
                     '{\"content\":\"vorpal blade\"}', 9, 'turn-b1');
            ",
        )
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();

        let results = search_content(&db, &index, "vorpal", None, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].uri, "cairn://p/proj/12/1/builder/chat");
        assert_eq!(results[0].turn_start, None);
    }

    #[tokio::test]
    async fn a_sub_agent_hit_addresses_its_task_transcript() {
        let db = migrated_db().await;
        seed_transcript(&db).await;
        db.execute_script(
            "
            INSERT INTO jobs(id, execution_id, parent_job_id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at)
             VALUES ('job-task', 'exec-1', 'job-1', 'issue-1', 'project-1', 'running', 'Explore', 'explore', 2, 2);
            INSERT INTO runs(id, issue_id, project_id, job_id, status, session_id, created_at, updated_at)
             VALUES ('run-task', 'issue-1', 'project-1', 'job-task', 'exited', 'session-task', 2, 2);
            INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
             VALUES ('turn-t1', 'session-task', 'run-task', 'job-task', 1, 'done', 2, 2);
            INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES ('ev-task', 'run-task', 'session-task', 1, 2, 'assistant',
                     '{\"content\":\"jabberwock findings\"}', 2, 'turn-t1');
            ",
        )
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();

        let results = search_content(&db, &index, "jabberwock", None, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].uri,
            "cairn://p/proj/12/1/builder/task/explore/chat/turn/1"
        );
        assert_eq!(results[0].node_segment.as_deref(), Some("builder"));
        assert_eq!(results[0].task_segment.as_deref(), Some("explore"));
        assert_eq!(results[0].title, "explore");
    }

    #[tokio::test]
    async fn search_content_keeps_workspace_posts_and_comments_navigable() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO posts(id, title, content, author_principal_json, appearance_snapshot_json, created_at)
             VALUES (41, 'Workspace note', 'globally searchable cairnbench', '{}', '{}', 1);
             INSERT INTO post_comments(id, post_id, content, author_principal_json, appearance_snapshot_json, created_at)
             VALUES (42, 41, 'cairnbench followup', '{}', '{}', 2);",
        )
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();
        let results = search_content(
            &db,
            &index,
            "cairnbench",
            Some(SearchFilters {
                content_types: Some(vec!["post".to_string(), "post_comment".to_string()]),
                limit: Some(10),
                ..Default::default()
            }),
            None,
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.project_id.is_empty()));
        assert_eq!(
            results
                .iter()
                .find(|result| result.content_type == SearchContentType::Post)
                .map(|result| result.uri.as_str()),
            Some("cairn://posts/41")
        );
        assert_eq!(
            results
                .iter()
                .find(|result| result.content_type == SearchContentType::PostComment)
                .map(|result| result.uri.as_str()),
            Some("cairn://posts")
        );
    }
}
