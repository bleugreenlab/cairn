//! Decision-route feedback and passive engagement, keyed by event.

use std::collections::HashMap;
use std::time::Duration;

use cairn_uri::{build_thread_uri, parse_uri, CairnResource};
use serde_json::Value;

use crate::orchestrator::Orchestrator;
use crate::storage::route_feedback::{
    self as feedback, addresses_from, NewRouteFeedback, RouteFeedbackRow,
};
use crate::storage::{route_state, FactRow, LocalDb, RouteFiringRecord, RowExt};

pub const SWEEP_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// How long after an event its passive engagement is judged.
pub const ENGAGEMENT_HORIZON_SECONDS: i64 = 48 * 60 * 60;
/// How long a dispatched fact with no firing yet may hold the cursor: a
/// non-admitted outcome is journaled only once its acknowledgement has sent.
const SETTLE_SECONDS: i64 = 10 * 60;
const BATCH: i64 = 200;

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
                    log::info!("route feedback sweep: {report:?}")
                }
                Ok(_) => {}
                Err(error) => log::warn!("route feedback sweep failed: {error}"),
            }
        }
    });
}

/// One pass: record every settled decision outcome past the cursor, then stamp
/// every row whose engagement horizon has passed. `now` is epoch seconds.
pub async fn sweep_once(orch: &Orchestrator, now: i64) -> Result<SweepReport, String> {
    let mut report = sweep_outcomes(orch, now).await?;
    report.engagement_checked = check_engagement(orch, now).await?;
    Ok(report)
}

/// The thread decision as the route's firing journal recorded it.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ThreadDecision {
    pub route_id: String,
    pub decision_node: String,
    pub question: String,
    pub question_text: Option<String>,
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
fn decision_from_entry(entry: &Value, route: &str, question: &str) -> Option<ThreadDecision> {
    let answer = entry.get("answers")?.get(question)?;
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
            .find(|candidate| candidate.get("name").and_then(Value::as_str) == Some(question))?
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
        route_id: route.to_string(),
        decision_node: entry.get("decision")?.as_str()?.to_string(),
        question: question.to_string(),
        question_text: entry
            .get("asked")
            .and_then(|a| a.get("questions"))
            .and_then(Value::as_array)
            .and_then(|questions| {
                questions
                    .iter()
                    .find(|q| q.get("name").and_then(Value::as_str) == Some(question))
            })
            .and_then(|q| q.get("instructions"))
            .and_then(Value::as_str)
            .map(str::to_owned),
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

fn routing_question(question: &Value) -> bool {
    if question.get("source").and_then(Value::as_str) == Some("chartered_threads") {
        return true;
    }
    // Resolved charter options replace the source declaration in the journal.
    question.get("options").and_then(Value::as_array).is_some_and(|options| {
        options.iter().any(|option| option.get("address").and_then(Value::as_str)
            .is_some_and(|uri| matches!(parse_uri(uri), Some(CairnResource::Thread { path, .. }) if path.is_empty())))
    })
}

/// The thread decision any of `firings` journaled; every firing of one fact
/// carries the same one.
fn decisions(firings: &[RouteFiringRecord]) -> Vec<ThreadDecision> {
    let mut found = Vec::new();
    for firing in firings {
        let history: Vec<Value> =
            serde_json::from_str(firing.transforms_json.as_deref().unwrap_or("[]"))
                .unwrap_or_default();
        for entry in history {
            let Some(answers) = entry.get("answers").and_then(Value::as_object) else {
                continue;
            };
            let questions = entry
                .get("asked")
                .and_then(|a| a.get("questions"))
                .and_then(Value::as_array);
            for question in answers.keys().filter(|name| {
                questions.is_some_and(|questions| {
                    questions.iter().any(|question| {
                        question.get("name").and_then(Value::as_str) == Some(name.as_str())
                            && routing_question(question)
                    })
                })
            }) {
                let Some(decision) = decision_from_entry(&entry, &firing.route_id, question) else {
                    continue;
                };
                if !found.iter().any(|d: &ThreadDecision| {
                    d.route_id == decision.route_id
                        && d.decision_node == decision.decision_node
                        && d.question == decision.question
                }) {
                    found.push(decision);
                }
            }
        }
    }
    found
}

/// The threads the route delivered `firings` to, in journal order.
pub(crate) fn delivered_threads(firings: &[RouteFiringRecord]) -> Vec<String> {
    let mut threads: Vec<String> = Vec::new();
    for firing in firings {
        if firing.sink_kind != "message" || firing.status != "fired" {
            continue;
        }
        if let Some(sink) = firing.sink_ref.as_deref() {
            if !matches!(parse_uri(sink), Some(CairnResource::Thread { path, .. }) if path.is_empty())
            {
                continue;
            }
            let thread = cairn_uri::canonicalize_uri_identity(sink);
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

/// The rows one dispatched fact's outcome produces: one per delivered thread when
/// admitted, otherwise one recipient-less row; a fact whose firings are gone
/// is counted as unrecorded without inventing a route identity.
pub(crate) fn outcome_rows(
    project_key: &str,
    fact: &FactRow,
    firings: &[RouteFiringRecord],
    now: i64,
) -> Vec<NewRouteFeedback> {
    let row = |recipient, decision: &ThreadDecision| NewRouteFeedback {
        project_key: project_key.to_string(),
        event_id: fact.id,
        route_id: decision.route_id.clone(),
        decision_node: decision.decision_node.clone(),
        question: decision.question.clone(),
        question_text: decision.question_text.clone(),
        source: fact.source.clone(),
        recipient_thread: recipient,
        outcome: decision.outcome.clone(),
        pick: decision.pick.clone(),
        probability: decision.probability,
        measured_by: decision.measured_by.clone(),
        revision: decision.revision.clone(),
        picks_json: Some(serde_json::to_string(&decision.picks).unwrap_or_default()),
        addresses_from: addresses_from::FIRINGS.to_string(),
        event_at: event_at(fact),
        swept_at: now,
    };
    let recorded = decisions(firings);
    let mut rows = Vec::new();
    for decision in &recorded {
        let recipients = delivered_threads(firings)
            .into_iter()
            .filter(|recipient| {
                decision
                    .addresses
                    .iter()
                    .any(|address| cairn_uri::canonicalize_uri_identity(address) == *recipient)
                    && firings.iter().any(|f| {
                        f.route_id == decision.route_id
                            && decisions(std::slice::from_ref(f)).iter().any(|d| {
                                d.decision_node == decision.decision_node
                                    && d.question == decision.question
                            })
                            && f.status == "fired"
                            && f.sink_kind == "message"
                            && f.sink_ref.as_deref().is_some_and(|sink| {
                                cairn_uri::canonicalize_uri_identity(sink) == *recipient
                            })
                    })
            })
            .collect::<Vec<_>>();
        if recipients.is_empty() || decision.outcome != "admitted" {
            rows.push(row(None, decision));
        } else {
            rows.extend(
                recipients
                    .into_iter()
                    .map(|recipient| row(Some(recipient), decision)),
            );
        }
    }
    rows
}

fn rows_for(
    project: &str,
    fact: &FactRow,
    firings: &[RouteFiringRecord],
    routes: &super::index::RouteSet,
    now: i64,
) -> Vec<NewRouteFeedback> {
    let mut rows = outcome_rows(project, fact, firings, now);
    if rows.iter().any(|r| r.question_text.is_none()) {
        for row in &mut rows {
            if row.question_text.is_some() {
                continue;
            }
            // Old journal entries may lack instructions. The current definition
            // is an explicitly approximate snapshot, not historical evidence.
            if let Some(route) = routes
                .routes_for(&fact.source)
                .find(|r| r.id == row.route_id)
            {
                if let Some(node) = route
                    .definition
                    .nodes
                    .iter()
                    .find(|n| n.id == row.decision_node)
                {
                    if let super::RouteNodeConfig::Decision { decision, .. } = &node.config {
                        row.question_text = serde_json::to_value(decision).ok().and_then(|value| {
                            value
                                .get("questions")
                                .and_then(Value::as_array)
                                .and_then(|qs| {
                                    qs.iter().find(|q| {
                                        q.get("name").and_then(Value::as_str)
                                            == Some(row.question.as_str())
                                    })
                                })
                                .and_then(|q| q.get("instructions"))
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        });
                    }
                }
            }
        }
    }
    rows
}

fn has_thread_sink(definition: &super::RouteDefinition) -> bool {
    definition.nodes.iter().any(|node| matches!(&node.config,
        super::RouteNodeConfig::Sink { sink: super::RouteSink::Message { target, .. } }
        if match target {
            super::Bindable::Fixed(uri) => matches!(parse_uri(uri), Some(CairnResource::Thread { path, .. }) if path.is_empty()),
            super::Bindable::Bound(_) => true,
        }))
}

async fn current_routes(
    orch: &Orchestrator,
    fact: &FactRow,
    now: i64,
) -> Result<std::sync::Arc<super::index::RouteSet>, String> {
    let path = super::worker::project_path(orch, fact.project_id.as_deref()).await;
    orch.fact_log
        .index
        .routes(&orch.config_dir, path.as_deref(), now)
}

fn feedback_firings(
    mut firings: Vec<RouteFiringRecord>,
    fact: &FactRow,
    routes: &super::index::RouteSet,
) -> Vec<RouteFiringRecord> {
    let eligible: Vec<_> = firings
        .iter()
        .filter(|firing| {
            if !delivered_threads(std::slice::from_ref(firing)).is_empty() {
                return true;
            }
            match routes
                .routes_for(&fact.source)
                .find(|route| route.id == firing.route_id)
            {
                Some(route) => has_thread_sink(&route.definition),
                None => false,
            }
        })
        .map(|f| f.route_id.clone())
        .collect();
    firings.retain(|f| eligible.contains(&f.route_id));
    firings
}

/// With the bounded journal gone, current route matching is only an estimate;
/// it never supplies a fabricated historical route/node identity.
async fn missing_decision_history(
    orch: &Orchestrator,
    fact: &FactRow,
    routes: &super::index::RouteSet,
    registry: &super::FactRegistry,
) -> Result<bool, String> {
    let clauses: Vec<_> = routes
        .routes_for(&fact.source)
        .filter(|route| {
            has_thread_sink(&route.definition)
                && route
                    .definition
                    .nodes
                    .iter()
                    .any(|n| n.config.is_decision())
        })
        .flat_map(|route| route.definition.triggers())
        .collect();
    if clauses.is_empty() {
        return Ok(false);
    }
    let needs_content = registry
        .source(&fact.source)
        .is_some_and(|spec| super::log::clauses_need_hydration(spec, clauses.iter().copied()));
    let stored = super::log::stored_fact(fact)?;
    let fields = if needs_content {
        match super::log::hydrate(orch, fact).await {
            Ok(hydrated) => hydrated.fact.fields,
            Err(error) => {
                log::info!("route feedback: cannot estimate missing history for fact {} source {}: {error}", fact.id, fact.source);
                return Ok(false);
            }
        }
    } else {
        stored.fields
    };
    let candidate = super::Fact {
        source: &fact.source,
        fields: &fields,
        presence: if fact.presence == "active" {
            super::Presence::Active
        } else {
            super::Presence::Away
        },
    };
    Ok(clauses
        .into_iter()
        .any(|clause| super::matches(std::slice::from_ref(clause), &candidate)))
}

async fn sweep_outcomes(orch: &Orchestrator, now: i64) -> Result<SweepReport, String> {
    let registry = super::FactRegistry::default();
    let local = &orch.db.local;
    let mut report = SweepReport::default();
    let mut cursor = feedback::sweep_cursor(local)
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let facts = crate::storage::list_all_facts_after(local, cursor, BATCH)
            .await
            .map_err(|error| error.to_string())?;
        let mut rows = Vec::new();
        let mut advanced = cursor;
        for fact in &facts {
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
            let raw = crate::storage::list_firings_for_fact(local, fact.id)
                .await
                .map_err(|e| e.to_string())?;
            let raw_missing = raw.is_empty();
            let routes = current_routes(orch, fact, now).await?;
            let firings = feedback_firings(raw, fact, &routes);
            let missing =
                raw_missing && missing_decision_history(orch, fact, &routes, &registry).await?;
            if missing && now - event_at(fact) < SETTLE_SECONDS {
                report.held_at = Some(fact.id);
                break;
            }
            let Some(project_key) = crate::routes::project_key(orch, fact).await else {
                advanced = fact.id;
                continue;
            };
            let produced = rows_for(&project_key, fact, &firings, &routes, now);
            if missing {
                log::info!("route feedback: fact {} source {} has no surviving decision journal; counted unrecorded without fabricating route identity", fact.id, fact.source);
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
    row: &RouteFeedbackRow,
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

async fn check_engagement(orch: &Orchestrator, now: i64) -> Result<u64, String> {
    let local = &orch.db.local;
    let due = feedback::pending_engagement(local, now - ENGAGEMENT_HORIZON_SECONDS, BATCH)
        .await
        .map_err(|error| error.to_string())?;
    let registry = super::FactRegistry::default();
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
                        let mut engaged = cited;
                        if !engaged {
                            if let Some(fact) = crate::storage::get_fact(local, row.event_id)
                                .await
                                .map_err(|e| e.to_string())?
                            {
                                if let Some(spec) = registry.source(&fact.source) {
                                    for signal in spec.engagement_signals {
                                        engaged |= signal(
                                            local,
                                            &fact,
                                            recipient,
                                            row.event_at,
                                            row.event_at + ENGAGEMENT_HORIZON_SECONDS,
                                        )
                                        .await?;
                                    }
                                }
                            }
                        }
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
    /// The verified desktop operator, through the channels door, attributed to
    /// this installation's machine principal exactly as an operator-created
    /// issue is.
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

/// A feedback write that speaks for no run and carries no operator credential:
/// nothing proves who sent it, so it labels nothing.
const RUN_LESS_LABEL_REFUSAL: &str = "Refused: this request speaks for no run and carries no operator credential, so it cannot label a decision routing. A chartered thread labels its own routings; the operator labels them from the desktop.";

/// The labeller of a feedback write. The verified desktop operator (the
/// channels door's stamp) labels as the operator; otherwise the write must
/// carry an authenticated run whose home is a chartered thread. A run-less
/// write without the operator's stamp, including one from a process holding
/// only the machine credential, is refused rather than trusted.
pub(crate) async fn labeller(
    orch: &Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Result<Labeller, String> {
    if request.is_desktop_operator() {
        let origin = crate::routes::installation_machine_origin(orch)?;
        return Ok(Labeller::Operator {
            principal_json: serde_json::to_string(&origin.author)
                .map_err(|error| error.to_string())?,
        });
    }
    if request.run_id.as_deref().is_none_or(str::is_empty) || request.is_unverified_local() {
        return Err(RUN_LESS_LABEL_REFUSAL.to_string());
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            format!("Only a chartered thread or the operator can label a decision routing, and this write's run could not be resolved: {error}")
        })?;
    match parse_uri(&agent.home) {
        Some(CairnResource::Thread { project, path, .. }) if path.is_empty() => {
            Ok(Labeller::Thread {
                uri: cairn_uri::canonicalize_uri_identity(&agent.home),
                project_key: project,
            })
        }
        _ => Err(format!(
            "Only a chartered thread can label a decision routing; this write came from {}, which is a task or node, not a thread.",
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
                Some(CairnResource::WorkspaceEvent { .. }) => Err(
                    "Project-less event URIs cannot be labelled: feedback requires a project."
                        .into(),
                ),
                _ => Err(refused()),
            }
        }
        _ => Err(refused()),
    }
}

/// The dispatched fact a label names, and its project.
async fn labelled_fact(
    orch: &Orchestrator,
    project: &str,
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
    let project_key = crate::routes::project_key(orch, &fact)
        .await
        .ok_or_else(|| {
            format!("{uri_named} belongs to no project, so it has no routing to label.")
        })?;
    if !project.eq_ignore_ascii_case(&project_key) {
        return Err(format!(
            "Event {id} belongs to project {project_key}, not writer project {project}."
        ));
    }
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
    if !matches!(
        fact.route_state.as_str(),
        route_state::DONE | route_state::FAILED | route_state::INTERRUPTED
    ) {
        return Err(format!(
            "{uri_named} was not dispatched by a decision route (state {}).",
            fact.route_state
        ));
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
fn original_citations(firings: &[RouteFiringRecord], row: &RouteFeedbackRow) -> String {
    if let Some(decision) = decisions(firings).into_iter().find(|d| {
        d.route_id == row.route_id
            && d.decision_node == row.decision_node
            && d.question == row.question
            && !d.citations.is_empty()
    }) {
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
    project: &str,
    labeller: &Labeller,
    payload: &LabelPayload,
    dry_run: bool,
) -> Result<String, String> {
    let local = &orch.db.local;
    let now = orch.services.clock.now();
    let (fact, project_key) = labelled_fact(orch, project, labeller, &payload.event).await?;
    let event = super::fact_row_uri(Some(&project_key), fact.id);
    let db = orch.db.for_project(&project_key).await;
    let project_id = project_id(&db, &project_key)
        .await?
        .ok_or_else(|| format!("Project {project_key} no longer exists."))?;
    let target = match payload.to.as_deref() {
        Some(to) => Some(reroute_target(&db, &project_id, &project_key, to, labeller).await?),
        None => None,
    };
    let raw = crate::storage::list_firings_for_fact(local, fact.id)
        .await
        .map_err(|e| e.to_string())?;
    let routes = current_routes(orch, &fact, now).await?;
    let firings = feedback_firings(raw, &fact, &routes);
    let existing = feedback::list(
        local,
        &feedback::RouteFeedbackFilter {
            project_key: Some(project_key.clone()),
            event_id: Some(fact.id),
            ..Default::default()
        },
        i64::MAX,
    )
    .await
    .map_err(|e| e.to_string())?;
    let produced = rows_for(&project_key, &fact, &firings, &routes, now);
    if decisions(&firings).is_empty() && existing.is_empty() {
        return Err(format!(
            "{event} has no dispatched decision routing to label."
        ));
    }
    let own_delivery = labeller.thread_uri().is_some_and(|uri| {
        existing
            .iter()
            .any(|r| r.recipient_thread.as_deref() == Some(uri))
            || produced
                .iter()
                .any(|r| r.recipient_thread.as_deref() == Some(uri))
    });
    if !own_delivery && payload.verdict == "not_mine" {
        return Err(format!("Nothing to disown: {event} was not delivered to {}, so not_mine has no delivery to label. To record where it ended up, write verdict reroute with to naming the thread that handled it.", labeller.thread_uri().unwrap_or("the operator")));
    }
    let recipient = if own_delivery {
        labeller.thread_uri()
    } else {
        None
    };
    let side = recipient.unwrap_or("the refused side");
    let mut selected: Vec<_> = existing
        .iter()
        .filter(|r| r.recipient_thread.as_deref() == recipient)
        .cloned()
        .collect();
    let delivers = own_delivery
        && target.as_ref().is_some_and(|(_, uri)| {
            !selected
                .iter()
                .any(|r| r.reroute_to.as_deref() == Some(uri))
        });
    if dry_run {
        return Ok(format!(
            "Would label {event} for {side}: {}{}.",
            payload.verdict,
            if delivers {
                "; would deliver the original message"
            } else {
                ""
            }
        ));
    }
    for mut row in produced {
        if !own_delivery {
            row.recipient_thread = None;
        }
        if row.recipient_thread.as_deref() != recipient {
            continue;
        }
        let stored = feedback::ensure(local, row)
            .await
            .map_err(|e| e.to_string())?;
        if !selected.iter().any(|r| r.id == stored.id) {
            selected.push(stored);
        }
    }
    if !own_delivery {
        for retained in &existing {
            let row = feedback::ensure(
                local,
                NewRouteFeedback {
                    project_key: retained.project_key.clone(),
                    event_id: retained.event_id,
                    route_id: retained.route_id.clone(),
                    decision_node: retained.decision_node.clone(),
                    question: retained.question.clone(),
                    question_text: retained.question_text.clone(),
                    source: retained.source.clone(),
                    recipient_thread: None,
                    outcome: retained.outcome.clone(),
                    pick: retained.pick.clone(),
                    probability: retained.probability,
                    measured_by: retained.measured_by.clone(),
                    revision: retained.revision.clone(),
                    picks_json: retained.picks_json.clone(),
                    addresses_from: retained.addresses_from.clone(),
                    event_at: retained.event_at,
                    swept_at: now,
                },
            )
            .await
            .map_err(|e| e.to_string())?;
            if !selected.iter().any(|r| r.id == row.id) {
                selected.push(row);
            }
        }
    }
    if selected.is_empty() {
        return Err(format!(
            "{event} has no surviving decision snapshot to label."
        ));
    }
    let mut delivered = String::new();
    if delivers {
        if let (Some((target_id, target_uri)), Some(from), Some(row)) =
            (&target, labeller.thread_uri(), selected.first())
        {
            match super::log::hydrate(orch, &fact).await {
                Ok(hydrated) => {
                    let text = hydrated.fact.fields.get("text").or_else(|| hydrated.fact.fields.get("body"))
                        .and_then(Value::as_str).map(str::to_owned)
                        .or(hydrated.fact.summary).unwrap_or_else(|| fact.record_ref.clone());
                    let body = format!("Re-routed from {from} (event {event}): originally Decision: {}\n\n{text}", original_citations(&firings, row));
                    crate::mcp::handlers::messages::append_thread_message(orch, request, &project_key, target_id, &body).await?;
                    delivered = format!(" Delivered the original message to {target_uri}.");
                }
                Err(error) => delivered = format!(" The original record could not be hydrated ({error}), so nothing was delivered to {target_uri}."),
            }
        }
    }
    feedback::set_labels(
        local,
        selected.iter().map(|row| row.id).collect(),
        feedback::RouteLabel {
            verdict: payload.verdict.clone(),
            reroute_to: target.as_ref().map(|(_, uri)| uri.clone()),
            note: payload.note.clone(),
            labelled_by: labeller.labelled_by(),
            labelled_at: now,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(format!(
        "Labelled {event} for {side}: {}.{delivered}",
        payload.verdict
    ))
}

#[cfg(test)]
#[path = "feedback_tests.rs"]
mod tests;
