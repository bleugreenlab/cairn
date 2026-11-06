//! The ambient routing label set's sweep (cairn/6290).
//!
//! The ambient Discord route classifies each unaddressed ambient message and
//! delivers it to the admitted threads. Calibrating its floors needs an outcome
//! for every routing, not only for the ones a thread chose to label, so this
//! sweep walks the fact log's ambient `channel_message` rows in id order and
//! records one `ambient_routing_feedback` row per delivered address, or one
//! recipient-less row for an outcome that delivered nowhere.
//!
//! The decision summary survives only in the route's firing journal, a
//! 200-row ring buffer per route, so the sweep runs every five minutes. A fact
//! whose firings were already evicted still gets one row, with outcome
//! `unrecorded` and no features, so losses are counted rather than silent; the
//! durable decision record that would replace them is cairn/6317.
//!
//! The same pass settles engagement 48 hours after each event, from exactly two
//! signals: the recipient thread authored a record citing the event (a thread
//! message, an issue, a post or post comment), or it sent agent text to the
//! originating conversation. Issue comments carry no author column, so they
//! cannot be attributed to a thread and are not searched. A refused-side row is
//! engaged when any thread cited the event, and names the earliest one.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use cairn_common::uri::{build_thread_uri, parse_uri, CairnResource};
use serde_json::Value;

use crate::orchestrator::Orchestrator;
use crate::storage::ambient_feedback::{
    self as feedback, addresses_from, AmbientFeedbackRow, NewAmbientFeedback, OUTCOME_UNRECORDED,
};
use crate::storage::{route_state, FactRow, LocalDb, RouteFiringRecord, RowExt};

/// The shipped ambient route's id: its file stem.
pub const AMBIENT_ROUTE: &str = "ambient-channel";
/// The decision question whose answer names the owning thread.
pub const THREAD_QUESTION: &str = "thread";
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// How long after an event its passive engagement is judged.
pub const ENGAGEMENT_HORIZON_SECONDS: i64 = 48 * 60 * 60;
/// How long a dispatched fact with no firing yet may hold the cursor: a
/// non-admitted outcome is journaled only once its acknowledgement has sent.
const SETTLE_SECONDS: i64 = 10 * 60;
const BATCH: i64 = 200;
const SOURCE: &str = "channel_message";

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub inserted: u64,
    pub filled: u64,
    pub unrecorded: u64,
    pub engagement_checked: u64,
    /// The fact the cursor is waiting on, when it stopped short.
    pub held_at: Option<i64>,
}

/// Run [`sweep_once`] every [`SWEEP_INTERVAL`]. Independent of the Discord
/// supervisor: the 48-hour checks are due whether or not Discord is up.
pub fn spawn(orch: Orchestrator) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let now = orch.services.clock.now();
            match sweep_once(&orch, now).await {
                Ok(report) if report != SweepReport::default() => {
                    log::info!("ambient feedback sweep: {report:?}")
                }
                Ok(_) => {}
                Err(error) => log::warn!("ambient feedback sweep failed: {error}"),
            }
        }
    });
}

/// One pass: record every settled ambient outcome past the cursor, then stamp
/// every row whose engagement horizon has passed. `now` is epoch seconds.
pub async fn sweep_once(orch: &Orchestrator, now: i64) -> Result<SweepReport, String> {
    let mut report = sweep_outcomes(orch, now).await?;
    report.engagement_checked = check_engagement(orch, now).await?;
    Ok(report)
}

fn fields(fact: &FactRow) -> BTreeMap<String, Value> {
    serde_json::from_str(&fact.fields_json).unwrap_or_default()
}

/// Whether `fact` is an unaddressed message in an ambient Discord binding:
/// exactly what the ambient route's trigger matches.
pub(crate) fn is_ambient_discord(fact: &FactRow) -> bool {
    let fields = fields(fact);
    fact.source == SOURCE
        && fields.get("bindingKind").and_then(Value::as_str) == Some("ambient")
        && fields.get("addressed").and_then(Value::as_bool) == Some(false)
        && fields.get("provider").and_then(Value::as_str) == Some("discord")
}

/// The conversation an ambient fact arrived in.
pub(crate) fn conversation_of(fact: &FactRow) -> Option<String> {
    fields(fact)
        .get("conversation")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// The ambient route's own firings for one fact.
pub(crate) async fn ambient_firings(
    db: &LocalDb,
    fact_id: i64,
) -> Result<Vec<RouteFiringRecord>, String> {
    Ok(crate::storage::list_firings_for_fact(db, fact_id)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|firing| firing.route_id == AMBIENT_ROUTE)
        .collect())
}

/// The thread decision as the route's firing journal recorded it.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ThreadDecision {
    pub outcome: String,
    pub pick: Option<String>,
    pub picks: Vec<String>,
    pub probability: Option<f64>,
    pub measured_by: Option<String>,
    pub revision: Option<String>,
    pub addresses: Vec<String>,
    pub citations: Vec<String>,
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// The `rev X` a citation such as `devops (0.9, native, rev abc)` carries.
fn citation_revision(citation: &str) -> Option<String> {
    let start = citation.rfind("rev ")? + 4;
    let rest = &citation[start..];
    let end = rest.find(')').unwrap_or(rest.len());
    Some(rest[..end].trim().to_string()).filter(|revision| !revision.is_empty())
}

/// Read the thread decision from one journaled decision entry: the outcome
/// and answers as summarized, the pick's own probability, and the pick's
/// charter revision from the options the question was asked over.
fn decision_from_entry(entry: &Value) -> Option<ThreadDecision> {
    let answer = entry.get("answers")?.get(THREAD_QUESTION)?;
    let outcome = answer.get("outcome")?.as_str()?.to_string();
    let pick = answer
        .get("pick")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let probability = pick
        .as_deref()
        .and_then(|pick| {
            answer
                .get("probabilities")?
                .as_array()?
                .iter()
                .find(|pair| pair.get(0).and_then(Value::as_str) == Some(pick))?
                .get(1)?
                .as_f64()
        })
        .or_else(|| answer.get("confidence").and_then(Value::as_f64));
    let citations = strings(answer.get("citations").unwrap_or(&Value::Null));
    let asked_revision = pick.as_deref().and_then(|pick| {
        entry
            .get("asked")?
            .get("questions")?
            .as_array()?
            .iter()
            .find(|question| question.get("name").and_then(Value::as_str) == Some(THREAD_QUESTION))?
            .get("options")?
            .as_array()?
            .iter()
            .find(|option| option.get("value").and_then(Value::as_str) == Some(pick))?
            .get("revision")?
            .as_str()
            .map(str::to_owned)
    });
    let revision = asked_revision.or_else(|| {
        answer
            .get("citation")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| citations.first().cloned())
            .and_then(|citation| citation_revision(&citation))
    });
    Some(ThreadDecision {
        outcome,
        pick,
        picks: strings(answer.get("picks").unwrap_or(&Value::Null)),
        probability,
        measured_by: answer
            .get("measuredBy")
            .and_then(Value::as_str)
            .map(str::to_owned),
        revision,
        addresses: strings(answer.get("addresses").unwrap_or(&Value::Null)),
        citations,
    })
}

/// The thread decision any of `firings` journaled; every firing of one fact
/// carries the same one.
pub(crate) fn thread_decision(firings: &[RouteFiringRecord]) -> Option<ThreadDecision> {
    firings.iter().find_map(|firing| {
        let history: Vec<Value> = serde_json::from_str(firing.transforms_json.as_deref()?).ok()?;
        history.iter().find_map(decision_from_entry)
    })
}

/// The threads the route delivered `firings` to, in journal order.
pub(crate) fn delivered_threads(firings: &[RouteFiringRecord]) -> Vec<String> {
    let mut threads: Vec<String> = Vec::new();
    for firing in firings {
        if firing.sink_kind != "message" || firing.status != "fired" {
            continue;
        }
        if let Some(sink) = firing.sink_ref.as_deref() {
            let thread = cairn_common::uri::canonicalize_uri_identity(sink);
            if !threads.contains(&thread) {
                threads.push(thread);
            }
        }
    }
    threads
}

/// A fact's mint time in epoch seconds (the log stores milliseconds).
pub(crate) fn event_at(fact: &FactRow) -> i64 {
    fact.minted_at / 1000
}

/// The rows one ambient fact's outcome produces: one per delivered thread when
/// admitted, otherwise one recipient-less row; a fact whose firings are gone
/// yields one `unrecorded` row with no features.
pub(crate) fn outcome_rows(
    project_key: &str,
    fact: &FactRow,
    firings: &[RouteFiringRecord],
    now: i64,
) -> Vec<NewAmbientFeedback> {
    let row = |recipient: Option<String>, decision: Option<&ThreadDecision>, source: &str| {
        NewAmbientFeedback {
            project_key: project_key.to_string(),
            event_id: fact.id,
            recipient_thread: recipient,
            outcome: decision.map_or_else(|| OUTCOME_UNRECORDED.to_string(), |d| d.outcome.clone()),
            pick: decision.and_then(|d| d.pick.clone()),
            probability: decision.and_then(|d| d.probability),
            measured_by: decision.and_then(|d| d.measured_by.clone()),
            revision: decision.and_then(|d| d.revision.clone()),
            picks_json: decision.map(|d| serde_json::to_string(&d.picks).unwrap_or_default()),
            addresses_from: source.to_string(),
            event_at: event_at(fact),
            swept_at: now,
        }
    };
    let Some(decision) = thread_decision(firings) else {
        return vec![row(None, None, addresses_from::NONE)];
    };
    if decision.outcome != "admitted" {
        return vec![row(None, Some(&decision), addresses_from::FIRINGS)];
    }
    let mut recipients = delivered_threads(firings);
    if recipients.is_empty() {
        recipients = decision
            .addresses
            .iter()
            .map(|address| cairn_common::uri::canonicalize_uri_identity(address))
            .collect();
    }
    if recipients.is_empty() {
        return vec![row(None, Some(&decision), addresses_from::FIRINGS)];
    }
    recipients
        .into_iter()
        .map(|recipient| row(Some(recipient), Some(&decision), addresses_from::FIRINGS))
        .collect()
}

async fn sweep_outcomes(orch: &Orchestrator, now: i64) -> Result<SweepReport, String> {
    let local = &orch.db.local;
    let mut report = SweepReport::default();
    let mut cursor = feedback::sweep_cursor(local)
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let facts = crate::storage::list_facts_after(local, SOURCE, cursor, BATCH)
            .await
            .map_err(|error| error.to_string())?;
        let mut rows = Vec::new();
        let mut advanced = cursor;
        for fact in &facts {
            if !is_ambient_discord(fact) {
                advanced = fact.id;
                continue;
            }
            match fact.route_state.as_str() {
                route_state::PENDING | route_state::QUEUED | route_state::RUNNING => {
                    report.held_at = Some(fact.id);
                    break;
                }
                // No route ran on it, so there is no routing outcome to label.
                route_state::UNMATCHED | route_state::STALE | route_state::ROUTE_GENERATED => {
                    advanced = fact.id;
                    continue;
                }
                _ => {}
            }
            let firings = ambient_firings(local, fact.id).await?;
            if firings.is_empty() && now - event_at(fact) < SETTLE_SECONDS {
                report.held_at = Some(fact.id);
                break;
            }
            let Some(project_key) = crate::routes::project_key(orch, fact).await else {
                advanced = fact.id;
                continue;
            };
            let produced = outcome_rows(&project_key, fact, &firings, now);
            if produced
                .iter()
                .any(|row| row.addresses_from == addresses_from::NONE)
            {
                log::info!(
                    "ambient feedback: fact {} reached after its firings were evicted; recorded as unrecorded",
                    fact.id
                );
                report.unrecorded += 1;
            }
            rows.extend(produced);
            advanced = fact.id;
        }
        if advanced > cursor || !rows.is_empty() {
            let counts = feedback::record_swept(local, rows, advanced)
                .await
                .map_err(|error| error.to_string())?;
            report.inserted += counts.inserted;
            report.filled += counts.filled;
            cursor = advanced;
        }
        if report.held_at.is_some() || (facts.len() as i64) < BATCH {
            return Ok(report);
        }
    }
}

/// Whether `text` cites fact `id`: its URI in either scope (`…/events/{id}`,
/// which also covers a `[#id](…/events/{id})` link), or `event #{id}` /
/// `event {id}` in prose. A bare number never counts, and neither does a longer
/// id that starts with this one.
pub(crate) fn cites_event(text: &str, id: i64) -> bool {
    let lower = text.to_lowercase();
    let id = id.to_string();
    let boundary_after = |end: usize| {
        !lower[end..]
            .chars()
            .next()
            .is_some_and(|next| next.is_ascii_digit())
    };
    let found = |needle: &str, word_start: bool| {
        lower.match_indices(needle).any(|(start, matched)| {
            let before_ok = !word_start
                || !lower[..start]
                    .chars()
                    .next_back()
                    .is_some_and(char::is_alphanumeric);
            before_ok && boundary_after(start + matched.len())
        })
    };
    found(&format!("events/{id}"), false)
        || found(&format!("event #{id}"), true)
        || found(&format!("event {id}"), true)
}

fn like_needles(id: i64) -> [String; 3] {
    [
        format!("%events/{id}%"),
        format!("%event #{id}%"),
        format!("%event {id}%"),
    ]
}

/// The project id for `key` in the database that owns the project.
async fn project_id(db: &LocalDb, key: &str) -> Result<Option<String>, String> {
    db.query_opt_text(
        "SELECT id FROM projects WHERE key = ?1",
        (key.to_lowercase(),),
    )
    .await
    .map_err(|error| error.to_string())
}

/// A record that might cite an event, with what attributes it to a thread.
struct Candidate {
    created_at: i64,
    text: String,
    /// The thread whose job sent it, when the record names a run directly.
    thread_id: Option<String>,
    /// The author principal JSON, for records that carry one.
    principal: Option<String>,
}

/// Every record in the project's database within `[from, to]` (seconds) whose
/// text may cite fact `id`, oldest first. The LIKE clauses only narrow the
/// candidates; [`cites_event`] decides.
async fn candidates(
    db: &LocalDb,
    project_id: &str,
    id: i64,
    from: i64,
    to: i64,
) -> Result<Vec<Candidate>, String> {
    let [a, b, c] = like_needles(id);
    let mut found = db
        .query_all(
            "SELECT m.created_at, m.content, j.thread_id FROM messages m
               JOIN runs r ON r.id = m.sender_run_id
               JOIN jobs j ON j.id = r.job_id
              WHERE m.created_at BETWEEN ?1 AND ?2 AND j.project_id = ?3 AND j.thread_id IS NOT NULL
                AND (m.content LIKE ?4 OR m.content LIKE ?5 OR m.content LIKE ?6)",
            (
                from,
                to,
                project_id.to_string(),
                a.clone(),
                b.clone(),
                c.clone(),
            ),
            |row| {
                Ok(Candidate {
                    created_at: row.i64(0)?,
                    text: row.text(1)?,
                    thread_id: row.opt_text(2)?,
                    principal: None,
                })
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let authored = |row: &cairn_db::turso::Row| {
        Ok(Candidate {
            created_at: row.i64(0)?,
            text: row.text(1)?,
            thread_id: None,
            principal: row.opt_text(2)?,
        })
    };
    found.extend(
        db.query_all(
            "SELECT created_at, title || char(10) || COALESCE(description, ''), author_principal_json FROM issues
              WHERE project_id = ?3 AND created_at BETWEEN ?1 AND ?2 AND author_principal_json IS NOT NULL
                AND (title LIKE ?4 OR title LIKE ?5 OR title LIKE ?6
                     OR description LIKE ?4 OR description LIKE ?5 OR description LIKE ?6)",
            (from, to, project_id.to_string(), a.clone(), b.clone(), c.clone()),
            authored,
        )
        .await
        .map_err(|error| error.to_string())?,
    );
    found.extend(
        db.query_all(
            "SELECT created_at, COALESCE(title, '') || char(10) || content, author_principal_json FROM posts
              WHERE (project_id = ?3 OR project_id IS NULL) AND created_at BETWEEN ?1 AND ?2
                AND (content LIKE ?4 OR content LIKE ?5 OR content LIKE ?6
                     OR title LIKE ?4 OR title LIKE ?5 OR title LIKE ?6)",
            (from, to, project_id.to_string(), a.clone(), b.clone(), c.clone()),
            authored,
        )
        .await
        .map_err(|error| error.to_string())?,
    );
    found.extend(
        db.query_all(
            "SELECT created_at, content, author_principal_json FROM post_comments
              WHERE created_at BETWEEN ?1 AND ?2 AND (content LIKE ?3 OR content LIKE ?4 OR content LIKE ?5)",
            (from, to, a, b, c),
            authored,
        )
        .await
        .map_err(|error| error.to_string())?,
    );
    found.retain(|candidate| cites_event(&candidate.text, id));
    found.sort_by_key(|candidate| candidate.created_at);
    Ok(found)
}

/// The thread (by id) an author principal names: its run's job's thread, or
/// its node when that node is one of `project_key`'s threads.
async fn principal_thread(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    principal: &str,
) -> Option<String> {
    let principal: Value = serde_json::from_str(principal).ok()?;
    if let Some(run_id) = principal.get("run_id").and_then(Value::as_str) {
        let thread = db
            .query_opt_text(
                "SELECT j.thread_id FROM runs r JOIN jobs j ON j.id = r.job_id WHERE r.id = ?1 AND j.project_id = ?2 AND j.thread_id IS NOT NULL",
                (run_id.to_string(), project_id.to_string()),
            )
            .await
            .ok()
            .flatten();
        if thread.is_some() {
            return thread;
        }
    }
    let node = principal.get("node").and_then(Value::as_str)?;
    thread_id_for_uri(db, project_id, project_key, node).await
}

/// The id of the thread `uri` addresses, when it is one of `project_key`'s.
pub(crate) async fn thread_id_for_uri(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    uri: &str,
) -> Option<String> {
    let Some(CairnResource::Thread {
        project,
        name,
        path,
    }) = parse_uri(uri)
    else {
        return None;
    };
    if !path.is_empty() || !project.eq_ignore_ascii_case(project_key) {
        return None;
    }
    crate::threads::crud::by_name(db, project_id, &name)
        .await
        .ok()
        .map(|thread| thread.id)
}

/// The thread ids that authored a record citing the event, earliest first.
async fn citing_threads(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    row: &AmbientFeedbackRow,
) -> Result<Vec<String>, String> {
    let window = (row.event_at, row.event_at + ENGAGEMENT_HORIZON_SECONDS);
    let mut threads = Vec::new();
    for candidate in candidates(db, project_id, row.event_id, window.0, window.1).await? {
        let thread = match (candidate.thread_id, candidate.principal) {
            (Some(thread), _) => Some(thread),
            (None, Some(principal)) => {
                principal_thread(db, project_id, project_key, &principal).await
            }
            (None, None) => None,
        };
        if let Some(thread) = thread {
            if !threads.contains(&thread) {
                threads.push(thread);
            }
        }
    }
    Ok(threads)
}

/// Whether `thread_uri` sent agent text to the event's conversation within the
/// horizon. Only `message`-kind rows qualify: a `route`-kind row is authored
/// by a route rather than a thread, so the issue's "agent text or route" sends
/// reduce to agent text.
async fn sent_to_conversation(
    local: &LocalDb,
    row: &AmbientFeedbackRow,
    thread_uri: &str,
) -> Result<bool, String> {
    let Some(fact) = crate::storage::get_fact(local, row.event_id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(false);
    };
    let Some(conversation) = conversation_of(&fact) else {
        return Ok(false);
    };
    let from_ms = row.event_at * 1000;
    let to_ms = (row.event_at + ENGAGEMENT_HORIZON_SECONDS) * 1000;
    let refs = local
        .query_all(
            "SELECT binding_ref FROM channel_outbound WHERE conversation = ?1 AND kind = ?2 AND created_at BETWEEN ?3 AND ?4",
            (
                conversation,
                crate::channels::agent_message::KIND.to_string(),
                from_ms,
                to_ms,
            ),
            |row| row.text(0),
        )
        .await
        .map_err(|error| error.to_string())?;
    let thread_uri = cairn_common::uri::canonicalize_uri_identity(thread_uri);
    Ok(refs.iter().any(|binding_ref| {
        crate::channels::agent_message::sender_home(binding_ref)
            .is_some_and(|home| cairn_common::uri::canonicalize_uri_identity(home) == thread_uri)
    }))
}

async fn check_engagement(orch: &Orchestrator, now: i64) -> Result<u64, String> {
    let local = &orch.db.local;
    let due = feedback::pending_engagement(local, now - ENGAGEMENT_HORIZON_SECONDS, BATCH)
        .await
        .map_err(|error| error.to_string())?;
    let mut projects: HashMap<String, Option<String>> = HashMap::new();
    let mut checked = 0;
    for row in due {
        let db = orch.db.for_project(&row.project_key).await;
        let project = match projects.get(&row.project_key) {
            Some(project) => project.clone(),
            None => {
                let found = project_id(&db, &row.project_key).await?;
                projects.insert(row.project_key.clone(), found.clone());
                found
            }
        };
        let (engaged, engaged_by) = match project {
            // A deleted project's rows are swept with it; until then nothing
            // can be attributed, so the row is settled unengaged.
            None => (false, None),
            Some(project_id) => {
                let citing = citing_threads(&db, &project_id, &row.project_key, &row).await?;
                match row.recipient_thread.as_deref() {
                    Some(recipient) => {
                        let recipient_id =
                            thread_id_for_uri(&db, &project_id, &row.project_key, recipient).await;
                        let cited = recipient_id.as_ref().is_some_and(|id| citing.contains(id));
                        let engaged = cited || sent_to_conversation(local, &row, recipient).await?;
                        (engaged, None)
                    }
                    None => match citing.first() {
                        Some(thread_id) => {
                            let by = crate::threads::crud::get(&db, thread_id)
                                .await
                                .map_err(|error| error.to_string())?
                                .map(|thread| build_thread_uri(&row.project_key, &thread.name));
                            (true, by)
                        }
                        None => (false, None),
                    },
                }
            }
        };
        feedback::stamp_engagement(local, row.id, engaged, engaged_by, now)
            .await
            .map_err(|error| error.to_string())?;
        checked += 1;
    }
    Ok(checked)
}

/// Who is labelling: a chartered thread (never a task or node), or the operator.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Labeller {
    Thread {
        uri: String,
        project_key: String,
    },
    /// A run-less write, attributed to this installation's machine principal
    /// exactly as an operator-created issue is.
    Operator {
        principal_json: String,
    },
}

impl Labeller {
    fn thread_uri(&self) -> Option<&str> {
        match self {
            Labeller::Thread { uri, .. } => Some(uri),
            Labeller::Operator { .. } => None,
        }
    }

    fn labelled_by(&self) -> String {
        match self {
            Labeller::Thread { uri, .. } => uri.clone(),
            Labeller::Operator { principal_json } => principal_json.clone(),
        }
    }
}

/// The labeller of a feedback write, from the request's authenticated run
/// alone. A write with a run must come from a thread itself; one without is
/// the operator's.
pub(crate) async fn labeller(
    orch: &Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Result<Labeller, String> {
    if request.run_id.is_none() {
        let origin = crate::routes::installation_machine_origin(orch)?;
        return Ok(Labeller::Operator {
            principal_json: serde_json::to_string(&origin.author)
                .map_err(|error| error.to_string())?,
        });
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            format!("Only a chartered thread or the operator can label an ambient routing, and this write's run could not be resolved: {error}")
        })?;
    match parse_uri(&agent.home) {
        Some(CairnResource::Thread { project, path, .. }) if path.is_empty() => {
            Ok(Labeller::Thread {
                uri: cairn_common::uri::canonicalize_uri_identity(&agent.home),
                project_key: project,
            })
        }
        _ => Err(format!(
            "Only a chartered thread can label an ambient routing; this write came from {}, which is a task or node, not a thread.",
            agent.home
        )),
    }
}

/// One feedback write's payload, as the gate admitted it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LabelPayload {
    pub event: Value,
    pub verdict: String,
    pub to: Option<String>,
    pub note: Option<String>,
}

fn optional_text(payload: &Value, key: &str) -> Result<Option<String>, String> {
    match payload.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.trim().is_empty() => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.trim().to_string())),
        Some(_) => Err(format!("payload.{key} must be a string.")),
    }
}

impl LabelPayload {
    /// The payload's shape, checked before anything is read: `reroute` needs
    /// `to`, and `not_mine` refuses one.
    pub(crate) fn parse(payload: &Value) -> Result<Self, String> {
        let verdict = optional_text(payload, "verdict")?
            .ok_or("payload.verdict is required: not_mine or reroute.")?;
        let to = optional_text(payload, "to")?;
        match (verdict.as_str(), &to) {
            ("not_mine", Some(_)) => {
                return Err("payload.to is refused with verdict not_mine: not_mine disowns the delivery without naming an owner; use verdict reroute to name the thread that owns it.".into())
            }
            ("reroute", None) => {
                return Err("payload.to is required with verdict reroute: name the chartered thread that owns the message.".into())
            }
            ("not_mine" | "reroute", _) => {}
            (other, _) => {
                return Err(format!(
                    "payload.verdict '{other}' is not a verdict; use not_mine or reroute."
                ))
            }
        }
        Ok(Self {
            event: payload.get("event").cloned().unwrap_or(Value::Null),
            verdict,
            to,
            note: optional_text(payload, "note")?,
        })
    }
}

/// The fact log id `event` names: an event URI in either scope, or the id.
fn event_id(event: &Value) -> Result<(i64, Option<String>), String> {
    let refused = || {
        format!(
            "payload.event must be an event URI (cairn://p/PROJECT/events/ID) or its id; got {event}."
        )
    };
    match event {
        Value::Number(number) => number
            .as_i64()
            .filter(|id| *id > 0)
            .map(|id| (id, None))
            .ok_or_else(refused),
        Value::String(text) => {
            let text = text.trim();
            if let Ok(id) = text.parse::<i64>() {
                return (id > 0).then_some((id, None)).ok_or_else(refused);
            }
            match parse_uri(text) {
                Some(CairnResource::ProjectEvent { project, id }) => Ok((id, Some(project))),
                Some(CairnResource::WorkspaceEvent { id }) => Ok((id, None)),
                _ => Err(refused()),
            }
        }
        _ => Err(refused()),
    }
}

/// The ambient fact a label names, and its project.
async fn labelled_fact(
    orch: &Orchestrator,
    labeller: &Labeller,
    event: &Value,
) -> Result<(FactRow, String), String> {
    let (id, named_project) = event_id(event)?;
    let uri_named = match &named_project {
        Some(project) => crate::routes::fact_row_uri(Some(project), id),
        None => format!("event {id}"),
    };
    let fact = crate::storage::get_fact(&orch.db.local, id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("{uri_named} is not in this runner's event log."))?;
    if !is_ambient_discord(&fact) {
        return Err(format!(
            "{uri_named} is a {} event, not an unaddressed message in an ambient Discord conversation; only ambient routings can be labelled.",
            fact.source
        ));
    }
    let project_key = crate::routes::project_key(orch, &fact)
        .await
        .ok_or_else(|| {
            format!("{uri_named} belongs to no project, so it has no routing to label.")
        })?;
    if let Some(named) = named_project.filter(|named| !named.eq_ignore_ascii_case(&project_key)) {
        return Err(format!(
            "{uri_named} names project {named}, but event {id} belongs to project {project_key}."
        ));
    }
    if let Labeller::Thread {
        project_key: own, ..
    } = labeller
    {
        if !own.eq_ignore_ascii_case(&project_key) {
            return Err(format!(
                "Event {id} belongs to project {project_key}; a thread of project {own} cannot label it."
            ));
        }
    }
    Ok((fact, project_key))
}

/// The chartered thread a reroute names, as `(id, canonical URI)`.
async fn reroute_target(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    to: &str,
    labeller: &Labeller,
) -> Result<(String, String), String> {
    let Some(CairnResource::Thread {
        project,
        name,
        path,
    }) = parse_uri(to)
    else {
        return Err(format!(
            "payload.to must be a thread URI such as cairn://p/{project_key}/THREAD; got {to}."
        ));
    };
    if !path.is_empty() {
        return Err(format!(
            "payload.to must name a thread itself, not one of its tasks; got {to}."
        ));
    }
    if !project.eq_ignore_ascii_case(project_key) {
        return Err(format!(
            "payload.to names a thread of project {project}, but the event belongs to project {project_key}."
        ));
    }
    let uri = build_thread_uri(project_key, &name);
    if labeller.thread_uri() == Some(uri.as_str()) {
        return Err(format!(
            "A thread cannot re-route a message to itself; {uri} is the labelling thread."
        ));
    }
    let thread = crate::threads::crud::by_name(db, project_id, &name)
        .await
        .map_err(|_| format!("{uri} is not a thread of project {project_key}."))?;
    if !crate::routes::decision::is_chartered_active(&thread) {
        return Err(format!(
            "{uri} is not an active chartered thread (it is closed or has no jurisdiction), so it cannot own a routed message."
        ));
    }
    Ok((thread.id, uri))
}

/// The citation form a re-route header repeats: the firing's own citations,
/// or, once those are evicted, the row's snapshot in the same shape.
fn original_citations(firings: &[RouteFiringRecord], row: &AmbientFeedbackRow) -> String {
    if let Some(decision) = thread_decision(firings).filter(|d| !d.citations.is_empty()) {
        return decision.citations.join(", ");
    }
    format!(
        "{} ({}, {}, rev {})",
        row.pick.as_deref().unwrap_or("no pick"),
        row.probability
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".into()),
        row.measured_by.as_deref().unwrap_or("-"),
        row.revision.as_deref().unwrap_or("-")
    )
}

/// Apply one feedback write. A thread labelling its own delivery sets the
/// label on its row, and a reroute there delivers the original message once to
/// the target thread. Anyone else (another thread, the operator) records where
/// the event ended up on its refused-side row, which delivers nothing.
pub(crate) async fn apply_label(
    orch: &Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    labeller: &Labeller,
    payload: &LabelPayload,
    dry_run: bool,
) -> Result<String, String> {
    let local = &orch.db.local;
    let now = orch.services.clock.now();
    let (fact, project_key) = labelled_fact(orch, labeller, &payload.event).await?;
    let event = crate::routes::fact_row_uri(Some(&project_key), fact.id);
    let db = orch.db.for_project(&project_key).await;
    let project_id = project_id(&db, &project_key)
        .await?
        .ok_or_else(|| format!("Project {project_key} no longer exists."))?;
    let target = match payload.to.as_deref() {
        Some(to) => Some(reroute_target(&db, &project_id, &project_key, to, labeller).await?),
        None => None,
    };
    let firings = ambient_firings(local, fact.id).await?;

    // The labeller's own delivered row: swept already, or created now from a
    // surviving firing that names it.
    let mut own = None;
    if let Some(uri) = labeller.thread_uri() {
        own = feedback::find(local, &project_key, fact.id, Some(uri))
            .await
            .map_err(|error| error.to_string())?;
        if own.is_none() && !dry_run && delivered_threads(&firings).iter().any(|t| t == uri) {
            if let Some(row) = outcome_rows(&project_key, &fact, &firings, now)
                .into_iter()
                .find(|row| row.recipient_thread.as_deref() == Some(uri))
            {
                own = Some(
                    feedback::ensure(local, row)
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }
        }
    }
    let own_delivery = own.is_some()
        || labeller
            .thread_uri()
            .is_some_and(|uri| delivered_threads(&firings).iter().any(|t| t == uri));
    if !own_delivery && payload.verdict == "not_mine" {
        return Err(format!(
            "Nothing to disown: {event} was not delivered to {}, so not_mine has no delivery to label. To record where it ended up, write verdict reroute with to naming the thread that handled it.",
            labeller.thread_uri().unwrap_or("the operator")
        ));
    }
    let side = if own_delivery {
        labeller.thread_uri().unwrap_or_default().to_string()
    } else {
        "the refused side".to_string()
    };
    let to_text = target
        .as_ref()
        .map(|(_, uri)| format!(" → {uri}"))
        .unwrap_or_default();
    let delivers = own_delivery
        && target.as_ref().is_some_and(|(_, uri)| {
            own.as_ref().and_then(|row| row.reroute_to.as_deref()) != Some(uri.as_str())
        });
    if dry_run {
        let delivery = match (&target, delivers) {
            (Some((_, uri)), true) => format!("; would deliver the original message to {uri}"),
            _ => String::new(),
        };
        return Ok(format!(
            "Would label {event} for {side}: {}{to_text}{delivery}.",
            payload.verdict
        ));
    }

    let row = match own {
        Some(row) => row,
        None => match feedback::find(local, &project_key, fact.id, None)
            .await
            .map_err(|error| error.to_string())?
        {
            Some(row) => row,
            None => {
                let decision = thread_decision(&firings);
                let swept = feedback::sweep_cursor(local)
                    .await
                    .map_err(|error| error.to_string())?
                    >= fact.id;
                let source = match (&decision, swept) {
                    (Some(_), _) => addresses_from::FIRINGS,
                    (None, true) => addresses_from::NONE,
                    (None, false) => addresses_from::LABEL,
                };
                feedback::ensure(
                    local,
                    NewAmbientFeedback {
                        project_key: project_key.clone(),
                        event_id: fact.id,
                        recipient_thread: None,
                        outcome: decision
                            .as_ref()
                            .map_or_else(|| OUTCOME_UNRECORDED.to_string(), |d| d.outcome.clone()),
                        pick: decision.as_ref().and_then(|d| d.pick.clone()),
                        probability: decision.as_ref().and_then(|d| d.probability),
                        measured_by: decision.as_ref().and_then(|d| d.measured_by.clone()),
                        revision: decision.as_ref().and_then(|d| d.revision.clone()),
                        picks_json: decision
                            .as_ref()
                            .map(|d| serde_json::to_string(&d.picks).unwrap_or_default()),
                        addresses_from: source.to_string(),
                        event_at: event_at(&fact),
                        swept_at: now,
                    },
                )
                .await
                .map_err(|error| error.to_string())?
            }
        },
    };

    let mut delivered = String::new();
    if delivers {
        if let (Some((target_id, target_uri)), Some(from)) = (&target, labeller.thread_uri()) {
            let text = match fact.record_id.as_deref() {
                Some(record) => crate::channels::ledger::get_inbound(local, record)
                    .await?
                    .map(|inbound| inbound.text),
                None => None,
            };
            match text {
                Some(text) => {
                    let body = format!(
                        "Re-routed from {from} (event {event}): originally Decision: {}\n\n{text}",
                        original_citations(&firings, &row)
                    );
                    crate::mcp::handlers::messages::append_thread_message(
                        orch,
                        request,
                        &project_key,
                        target_id,
                        &body,
                    )
                    .await?;
                    delivered = format!(" Delivered the original message to {target_uri}.");
                }
                None => {
                    delivered = format!(
                        " The original message is no longer in the inbound ledger, so nothing was delivered to {target_uri}."
                    )
                }
            }
        }
    }
    feedback::set_label(
        local,
        row.id,
        feedback::AmbientLabel {
            verdict: payload.verdict.clone(),
            reroute_to: target.as_ref().map(|(_, uri)| uri.clone()),
            note: payload.note.clone(),
            labelled_by: labeller.labelled_by(),
            labelled_at: now,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(format!(
        "Labelled {event} for {side}: {}{to_text}.{delivered}",
        payload.verdict
    ))
}

#[cfg(test)]
#[path = "ambient_feedback_tests.rs"]
mod tests;
