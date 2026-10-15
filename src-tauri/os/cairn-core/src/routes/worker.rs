//! The fact log worker: consumes `fact_log` in id order through two lanes.
//!
//! The **index lane** visits every new row once. It wakes the threads whose
//! standing triggers match (through the ordinary targeted wake path, so
//! dormancy, fencing, and mute rules apply) and decides whether any route
//! trigger matches, queueing the row for the route lane if one does. It never
//! runs a route, so a slow route cannot delay a thread wake.
//!
//! The **route lane** runs matched rows one at a time, in id order (which
//! keeps a followed thread's stream in order), each under a timeout. It hands
//! the channel submissions a row produces to the channel routers, choosing
//! recipients by the sink's destination or the source's [`Audience`].
//!
//! A source may also declare **addressee** fields: URIs naming who a row is
//! for (a post's mentions, a commented post's author). The index lane expands
//! them to the jobs they reach now and wakes those first, with no standing
//! trigger, then runs the clause consumers against the same set of jobs
//! already woken, so each recipient is woken at most once per row.
//!
//! Thread wakes are **coalesced**: the index lane folds each matching row into
//! the thread's pending wake for that source (`fact_log_wakes`), and the
//! index lane's tick delivers a wake once its settle window closes, so a
//! 30-file commit is one wake carrying a count and an id range, not 30.
//!
//! Dispatch is at-most-once: a row the runner was dispatching when it stopped
//! becomes `interrupted` on restart and is never re-fired, and so does a
//! coalesced wake caught mid-delivery.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;

use super::log::{clauses_need_hydration, hydrate, stored_fact, Hydrated};
use super::predicate::{scope_exclusion, ThreadScope};
use super::{matches, Audience, Fact, FactRegistry, RouteContext};
use crate::models::DeliveryUrgency;
use crate::orchestrator::wakes::{
    MutedDelivery, WakeDelivery, WakeEvent, WakeRouteAction, WakeSource, ADDRESSED_FACT_REF_PREFIX,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{route_state, FactRow, FactWake, WakeMatch, WakeWindow};

const INDEX_TICK: Duration = Duration::from_secs(2);
const PRUNE_EVERY: Duration = Duration::from_secs(3600);
const PRUNE_BATCH: i64 = 1000;
const MAX_RECORDED_WAKES: usize = 32;
const DAY_MS: i64 = 86_400_000;

/// The worker's bounds, from the workspace `eventLog` settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventLogPolicy {
    pub(crate) default_retention_ms: i64,
    pub(crate) retention_ms: HashMap<String, i64>,
    pub(crate) dispatch_timeout: Duration,
    pub(crate) max_dispatch_age_ms: i64,
    pub(crate) default_wake_settle_ms: i64,
    pub(crate) wake_settle_ms: HashMap<String, i64>,
    pub(crate) wake_max_window_ms: i64,
}

impl Default for EventLogPolicy {
    fn default() -> Self {
        Self {
            default_retention_ms: 30 * DAY_MS,
            retention_ms: HashMap::from([("file_changed".to_string(), 14 * DAY_MS)]),
            dispatch_timeout: Duration::from_secs(120),
            max_dispatch_age_ms: 15 * 60_000,
            default_wake_settle_ms: 30_000,
            wake_settle_ms: HashMap::from([("file_changed".to_string(), 60_000)]),
            wake_max_window_ms: 5 * 60_000,
        }
    }
}

impl EventLogPolicy {
    pub(crate) fn load(config_dir: &std::path::Path) -> Self {
        let settings = crate::config::settings::load_event_log_settings(config_dir);
        let mut policy = Self::default();
        let parse = |key: &str, value: &str| match crate::duration::parse_duration_ms(value) {
            Ok(ms) if ms > 0 => Some(ms.min(i64::MAX as u64) as i64),
            _ => {
                log::warn!("ignoring eventLog.{key} = {value:?}: not a positive duration");
                None
            }
        };
        let per_source = |key: &str,
                          values: &HashMap<String, String>,
                          default: &mut i64,
                          by_source: &mut HashMap<String, i64>| {
            for (source, value) in values {
                let Some(ms) = parse(&format!("{key}.{source}"), value) else {
                    continue;
                };
                if source == "default" {
                    *default = ms;
                } else {
                    by_source.insert(source.clone(), ms);
                }
            }
        };
        per_source(
            "retention",
            &settings.retention,
            &mut policy.default_retention_ms,
            &mut policy.retention_ms,
        );
        per_source(
            "wakeSettle",
            &settings.wake_settle,
            &mut policy.default_wake_settle_ms,
            &mut policy.wake_settle_ms,
        );
        if let Some(ms) = settings
            .wake_max_window
            .as_deref()
            .and_then(|value| parse("wakeMaxWindow", value))
        {
            policy.wake_max_window_ms = ms;
        }
        if let Some(ms) = settings
            .dispatch_timeout
            .as_deref()
            .and_then(|value| parse("dispatchTimeout", value))
        {
            policy.dispatch_timeout = Duration::from_millis(ms as u64);
        }
        if let Some(ms) = settings
            .max_dispatch_age
            .as_deref()
            .and_then(|value| parse("maxDispatchAge", value))
        {
            policy.max_dispatch_age_ms = ms;
        }
        policy
    }

    /// The settle window and cap for a coalesced wake on `source`.
    pub(crate) fn wake_window(&self, source: &str) -> WakeWindow {
        WakeWindow {
            settle_ms: self
                .wake_settle_ms
                .get(source)
                .copied()
                .unwrap_or(self.default_wake_settle_ms),
            max_window_ms: self.wake_max_window_ms,
        }
    }
}

/// Start both lanes and the hourly prune. Started once per runner.
pub fn spawn_fact_log_worker(orch: Orchestrator) {
    tokio::spawn(async move {
        let policy = EventLogPolicy::load(&orch.config_dir);
        let now = chrono::Utc::now().timestamp_millis();
        match crate::storage::recover_fact_log(
            &orch.db.local,
            now,
            now.saturating_sub(policy.max_dispatch_age_ms),
        )
        .await
        {
            Ok(recovery)
                if recovery.interrupted + recovery.stale + recovery.interrupted_wakes > 0 =>
            {
                log::info!(
                    "fact log recovered {} interrupted and {} stale rows, and {} interrupted wakes",
                    recovery.interrupted,
                    recovery.stale,
                    recovery.interrupted_wakes
                )
            }
            Ok(_) => {}
            Err(error) => log::warn!("fact log recovery failed: {error}"),
        }
        tokio::spawn(index_loop(orch.clone(), policy.clone()));
        tokio::spawn(route_loop(orch, policy));
    });
}

async fn index_loop(orch: Orchestrator, policy: EventLogPolicy) {
    let mut last_prune: Option<std::time::Instant> = None;
    loop {
        let pass_orch = orch.clone();
        let pass_policy = policy.clone();
        // Pending wakes persist, so the first pass after a restart delivers
        // whatever came due while the runner was down.
        let pass = crate::orchestrator::base_advance::supervised_sweep_pass(async move {
            if let Err(error) = index_pass(&pass_orch, &pass_policy).await {
                log::warn!("fact log index pass failed: {error}");
            }
            let now = chrono::Utc::now().timestamp_millis();
            if let Err(error) = flush_due_wakes(&pass_orch, now).await {
                log::warn!("fact log wake flush failed: {error}");
            }
        });
        if let Err(error) = pass.await {
            log::warn!("{error}");
        }
        if last_prune.is_none_or(|at| at.elapsed() >= PRUNE_EVERY) {
            last_prune = Some(std::time::Instant::now());
            if let Err(error) = prune_pass(&orch, &policy).await {
                log::warn!("fact log prune failed: {error}");
            }
        }
        tokio::select! {
            () = orch.fact_log.notify.notified() => {}
            () = tokio::time::sleep(INDEX_TICK) => {}
        }
    }
}

async fn route_loop(orch: Orchestrator, policy: EventLogPolicy) {
    loop {
        let pass_orch = orch.clone();
        let pass_policy = policy.clone();
        let pass = crate::orchestrator::base_advance::supervised_sweep_pass(async move {
            if let Err(error) = route_pass(&pass_orch, &pass_policy).await {
                log::warn!("fact log route pass failed: {error}");
            }
        });
        if let Err(error) = pass.await {
            log::warn!("{error}");
        }
        tokio::select! {
            () = orch.fact_log.route_notify.notified() => {}
            () = tokio::time::sleep(INDEX_TICK) => {}
        }
    }
}

/// Index every unindexed row. Returns how many rows it indexed.
pub(crate) async fn index_pass(
    orch: &Orchestrator,
    policy: &EventLogPolicy,
) -> Result<usize, String> {
    let mut indexed = 0;
    while let Some(row) =
        crate::storage::claim_for_index(&orch.db.local, chrono::Utc::now().timestamp_millis())
            .await
            .map_err(|error| error.to_string())?
    {
        let id = row.id;
        if let Err(error) = index_row(orch, policy, row).await {
            crate::storage::set_index_outcome(
                &orch.db.local,
                id,
                route_state::FAILED,
                Some(&format!("index: {error}")),
                None,
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        indexed += 1;
    }
    Ok(indexed)
}

/// Dispatch every queued row. Returns how many rows it claimed.
pub(crate) async fn route_pass(
    orch: &Orchestrator,
    policy: &EventLogPolicy,
) -> Result<usize, String> {
    let mut claimed = 0;
    loop {
        let stale_before = chrono::Utc::now().timestamp_millis() - policy.max_dispatch_age_ms;
        let Some(row) = crate::storage::claim_next_queued(&orch.db.local, stale_before)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(claimed);
        };
        claimed += 1;
        let id = row.id;
        let (state, note) = run_route_row(orch, row, policy.dispatch_timeout).await;
        crate::storage::finish_fact(&orch.db.local, id, state, note.as_deref())
            .await
            .map_err(|error| error.to_string())?;
    }
}

/// The canonical URI of one fact log row.
pub(crate) fn fact_row_uri(project_key: Option<&str>, id: i64) -> String {
    match project_key {
        Some(project) => cairn_common::uri::build_project_event_uri(project, id),
        None => cairn_common::uri::build_workspace_event_uri(id),
    }
}

pub(crate) async fn project_key(orch: &Orchestrator, row: &FactRow) -> Option<String> {
    let project_id = row.project_id.clone()?;
    orch.db
        .local
        .query_opt_text("SELECT key FROM projects WHERE id = ?1", (project_id,))
        .await
        .ok()
        .flatten()
        .or_else(|| {
            serde_json::from_str::<HashMap<String, Value>>(&row.fields_json)
                .ok()
                .and_then(|fields| fields.get("project")?.as_str().map(str::to_owned))
        })
}

async fn project_path(orch: &Orchestrator, project_id: Option<&str>) -> Option<PathBuf> {
    let project_id = project_id?.to_owned();
    orch.db
        .local
        .query_opt_text(
            "SELECT repo_path FROM projects WHERE id = ?1",
            (project_id,),
        )
        .await
        .ok()
        .flatten()
        .map(PathBuf::from)
}

fn wake_action(action: &WakeRouteAction) -> &'static str {
    match action {
        WakeRouteAction::Delivered => "delivered",
        WakeRouteAction::Suppressed => "suppressed",
        WakeRouteAction::Dropped => "dropped",
    }
}

async fn index_row(
    orch: &Orchestrator,
    policy: &EventLogPolicy,
    row: FactRow,
) -> Result<(), String> {
    let registry = FactRegistry::default();
    let Some(spec) = registry.source(&row.source) else {
        crate::storage::set_index_outcome(
            &orch.db.local,
            row.id,
            route_state::UNMATCHED,
            Some("source is not in the fact registry"),
            None,
        )
        .await
        .map_err(|error| error.to_string())?;
        return Ok(());
    };
    let path = project_path(orch, row.project_id.as_deref()).await;
    let routes = orch
        .fact_log
        .index
        .routes(&orch.config_dir, path.as_deref())?;
    let consumers = orch.fact_log.index.threads(&orch.db.local).await?;
    let presence = super::log::presence_from_str(&row.presence);
    let stored = stored_fact(&row)?;
    // Jurisdiction reads only stored fields (`project` is always stored), so
    // a consumer outside it never forces hydration.
    let threads: Vec<&super::index::ThreadConsumer> = {
        let stored = Fact {
            source: &stored.source,
            fields: &stored.fields,
            presence,
        };
        consumers
            .get(&row.source)
            .into_iter()
            .flatten()
            .filter(|consumer| consumer.admits(&stored, row.project_id.as_deref()))
            .collect()
    };

    let actors = actor_jobs(orch, &row, spec, &stored.fields).await;
    // One pending wake per (job, source): a job a row reaches by several
    // routes (two addressee fields, an addressee and a clause, two clauses) is
    // folded in once, under the first, which is the most specific.
    let window = policy.wake_window(&row.source);
    let mut woken: Vec<String> = Vec::new();
    let mut wakes = Vec::new();
    let mut unresolved = Vec::new();
    {
        let stored = Fact {
            source: &stored.source,
            fields: &stored.fields,
            presence,
        };
        for field in spec.addressees {
            for uri in addressee_uris(stored.fields, field) {
                let jobs =
                    match crate::orchestrator::wakes::addressed_jobs(&orch.db.local, &uri).await {
                        Ok(jobs) => jobs,
                        Err(error) => {
                            unresolved.push(format!("{field} {uri}: {error}"));
                            continue;
                        }
                    };
                for job in jobs {
                    if actors.contains(&job) || woken.contains(&job) {
                        continue;
                    }
                    let (home_project_id, home_project_key) =
                        match crate::orchestrator::wakes::job_project(&orch.db.local, &job).await {
                            Ok(project) => project,
                            Err(error) => {
                                unresolved.push(format!("{field} {uri}: {error}"));
                                continue;
                            }
                        };
                    let scope = ThreadScope {
                        home_project_id: home_project_id.as_deref(),
                        home_project_key: home_project_key.as_deref(),
                        row_project_id: row.project_id.as_deref(),
                    };
                    if scope_exclusion(None, &stored, &scope).is_some() {
                        continue;
                    }
                    woken.push(job.clone());
                    fold_wake(
                        orch,
                        &mut wakes,
                        WakeMatch {
                            job_id: job,
                            source: row.source.clone(),
                            clause_ref: format!(
                                "{ADDRESSED_FACT_REF_PREFIX}{}.{field}",
                                row.source
                            ),
                            project_id: row.project_id.clone(),
                            row_id: row.id,
                            at_ms: row.minted_at,
                            addressed: true,
                        },
                        window,
                        Some(field),
                    )
                    .await;
                }
            }
        }
    }

    let needs_content = clauses_need_hydration(
        spec,
        routes
            .clauses_for(&row.source)
            .chain(threads.iter().map(|consumer| &consumer.clause)),
    );
    let mut note = (!unresolved.is_empty())
        .then(|| format!("unresolved addressees: {}", unresolved.join("; ")));
    let fact = if needs_content {
        match hydrate(orch, &row).await {
            Ok(Hydrated { fact, .. }) => fact,
            Err(error) => {
                let hydration = format!("matched without hydrated fields: {error}");
                note = Some(match note {
                    Some(earlier) => format!("{earlier}; {hydration}"),
                    None => hydration,
                });
                stored
            }
        }
    } else {
        stored
    };
    let candidate = Fact {
        source: &fact.source,
        fields: &fact.fields,
        presence,
    };

    for consumer in &threads {
        if actors.contains(&consumer.job_id)
            || woken.contains(&consumer.job_id)
            || !matches(std::slice::from_ref(&consumer.clause), &candidate)
        {
            continue;
        }
        woken.push(consumer.job_id.clone());
        fold_wake(
            orch,
            &mut wakes,
            WakeMatch {
                job_id: consumer.job_id.clone(),
                source: row.source.clone(),
                clause_ref: consumer.clause_ref.clone(),
                project_id: row.project_id.clone(),
                row_id: row.id,
                at_ms: row.minted_at,
                addressed: false,
            },
            window,
            None,
        )
        .await;
    }

    let state = if fact.is_route_generated() {
        route_state::ROUTE_GENERATED
    } else if routes
        .clauses_for(&row.source)
        .any(|clause| matches(std::slice::from_ref(clause), &candidate))
    {
        route_state::QUEUED
    } else {
        route_state::UNMATCHED
    };
    let wakes_json = (!wakes.is_empty())
        .then(|| serde_json::to_string(&wakes))
        .transpose()
        .map_err(|error| error.to_string())?;
    crate::storage::set_index_outcome(
        &orch.db.local,
        row.id,
        state,
        note.as_deref(),
        wakes_json.as_deref(),
    )
    .await
    .map_err(|error| error.to_string())?;
    if state == route_state::QUEUED {
        orch.fact_log.route_notify.notify_one();
    }
    Ok(())
}

/// The jobs whose own write a row is: the job that minted it, and the job the
/// source's declared actor home resolves to now. Resolving the home here, as
/// the recipients are, is what keeps a home that rotated sessions between mint
/// and indexing from being handed its own writing on the new session.
async fn actor_jobs(
    orch: &Orchestrator,
    row: &FactRow,
    spec: &super::SourceSpec,
    fields: &BTreeMap<String, Value>,
) -> Vec<String> {
    let mut jobs: Vec<String> = row.actor_job_id.iter().cloned().collect();
    let home = spec
        .actor_home
        .and_then(|field| fields.get(field))
        .and_then(Value::as_str);
    if let Some(home) = home {
        match crate::orchestrator::wakes::addressed_jobs(&orch.db.local, home).await {
            Ok(found) => {
                for job in found {
                    if !jobs.contains(&job) {
                        jobs.push(job);
                    }
                }
            }
            Err(error) => log::warn!(
                "could not resolve actor home {home} of fact {}: {error}",
                row.id
            ),
        }
    }
    jobs
}

/// The URIs one addressee field of a stored fact holds: a scalar or a list.
fn addressee_uris(fields: &BTreeMap<String, Value>, field: &str) -> Vec<String> {
    match fields.get(field) {
        Some(Value::String(uri)) => vec![uri.clone()],
        Some(Value::Array(uris)) => uris
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

/// Fold one match into its job's pending wake and record it on the row.
/// `via` names the addressee field that reached the job, when one did.
async fn fold_wake(
    orch: &Orchestrator,
    wakes: &mut Vec<Value>,
    matched: WakeMatch,
    window: WakeWindow,
    via: Option<&str>,
) {
    let job = matched.job_id.clone();
    let (action, wake) =
        match crate::storage::coalesce_fact_wake(&orch.db.local, matched, window).await {
            Ok(wake) => ("coalesced".to_string(), Some(wake)),
            Err(error) => (format!("failed: {error}"), None),
        };
    if wakes.len() >= MAX_RECORDED_WAKES {
        return;
    }
    let home = crate::jobs::queries::home_uri_for_job(&orch.db.local, &job)
        .await
        .ok()
        .flatten();
    let mut record = serde_json::json!({
        "job": job,
        "uri": home,
        "action": action,
        "wake": wake,
    });
    if let Some(via) = via {
        record["via"] = Value::String(via.to_string());
    }
    wakes.push(record);
}

/// Deliver every coalesced wake due by `due_by_ms`. Returns how many it
/// claimed. Each wake is claimed before it is sent, so a wake goes out at
/// most once even across a restart.
pub(crate) async fn flush_due_wakes(orch: &Orchestrator, due_by_ms: i64) -> Result<usize, String> {
    let mut flushed = 0;
    while let Some(wake) = crate::storage::claim_due_fact_wake(
        &orch.db.local,
        due_by_ms,
        chrono::Utc::now().timestamp_millis(),
    )
    .await
    .map_err(|error| error.to_string())?
    {
        flushed += 1;
        let action = deliver_wake(orch, &wake).await;
        crate::storage::finish_fact_wake(&orch.db.local, wake.id, &action)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(flushed)
}

async fn deliver_wake(orch: &Orchestrator, wake: &FactWake) -> String {
    let (message, detail_uri) = wake_message(orch, wake).await;
    let muted = FactRegistry::default()
        .source(&wake.source)
        .map_or(MutedDelivery::Drop, |spec| spec.muted);
    let action = crate::orchestrator::wakes::route_wake(
        orch,
        WakeEvent {
            source: WakeSource::Fact {
                clause_ref: wake.clause_ref.clone(),
            },
            fact_kind: wake.source.clone(),
            detail_uri: Some(detail_uri),
            delivery: WakeDelivery::Targeted {
                subscriber_job_id: wake.job_id.clone(),
                sender_name: None,
                message,
            },
            urgency: DeliveryUrgency::Queue,
            muted,
        },
    )
    .await;
    match action {
        Ok(action) => wake_action(&action).to_string(),
        Err(error) => format!("failed: {error}"),
    }
}

/// A coalesced wake's message and detail URI: its header, then the writing the
/// newest row points at when its source carries some.
async fn wake_message(orch: &Orchestrator, wake: &FactWake) -> (String, String) {
    let (header, uri) = wake_header(orch, wake).await;
    match wake_body(orch, wake).await {
        Some(body) => (format!("{header}\n\n{body}"), uri),
        None => (header, uri),
    }
}

/// The content a wake carries inline, rendered from the canonical record the
/// newest row points at: only that row is certain to be this recipient's
/// match, since a range can interleave other recipients' rows. One arm per
/// source whose record is writing a recipient classifies by reading; every
/// other source's header is its whole message.
async fn wake_body(orch: &Orchestrator, wake: &FactWake) -> Option<String> {
    use crate::orchestrator::wakes::{POST_COMMENTED, POST_CREATED};
    if !matches!(wake.source.as_str(), POST_CREATED | POST_COMMENTED) {
        return None;
    }
    let row = crate::storage::get_fact(&orch.db.local, wake.last_row_id)
        .await
        .ok()??;
    crate::orchestrator::wakes::render_post_wake(&orch.db.local, &row.record_ref).await
}

/// A coalesced wake's header line and detail URI. One row reads as one event,
/// citing the row; several cite the id range of the events view that holds
/// them all.
async fn wake_header(orch: &Orchestrator, wake: &FactWake) -> (String, String) {
    if wake.count == 1 {
        if let Ok(Some(row)) = crate::storage::get_fact(&orch.db.local, wake.first_row_id).await {
            let uri = fact_row_uri(project_key(orch, &row).await.as_deref(), row.id);
            let summary = row.summary.as_deref().unwrap_or(row.identity.as_str());
            return (format!("[Event] {summary} ({}): {uri}", row.source), uri);
        }
    }
    let project = match wake.project_id.clone() {
        Some(id) => orch
            .db
            .local
            .query_opt_text("SELECT key FROM projects WHERE id = ?1", (id,))
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let (events, scope) = match project.as_deref() {
        Some(key) => (
            cairn_common::uri::build_project_events_uri(key),
            key.to_string(),
        ),
        None => (
            cairn_common::uri::build_workspace_events_uri(),
            "the workspace".to_string(),
        ),
    };
    let uri = format!(
        "{events}?source={}&from={}&to={}",
        wake.source, wake.first_row_id, wake.last_row_id
    );
    let rows = if wake.count == 1 { "row" } else { "rows" };
    (
        format!(
            "[Event] {} {} {rows} in {scope} (ids {}\u{2013}{}): {uri}",
            wake.count, wake.source, wake.first_row_id, wake.last_row_id
        ),
        uri,
    )
}

/// One standing trigger clause of a thread, evaluated against one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClauseVerdict {
    pub(crate) clause_ref: String,
    /// Why the row would not wake the thread through this clause; empty when
    /// it would.
    pub(crate) reasons: Vec<String>,
}

/// Evaluate the standing triggers of the thread at `home_uri` on `row`'s
/// source exactly as the index lane would: the same consumers, hydration,
/// scope rule, and matcher, so a reason given here is the reason the lane
/// acted on. Empty when the thread has no trigger on the row's source.
pub(crate) async fn explain_thread_triggers(
    orch: &Orchestrator,
    row: &FactRow,
    home_uri: &str,
) -> Result<Vec<ClauseVerdict>, String> {
    let consumers = orch.fact_log.index.threads(&orch.db.local).await?;
    let mut own = Vec::new();
    for consumer in consumers.get(&row.source).into_iter().flatten() {
        let home = crate::jobs::queries::home_uri_for_job(&orch.db.local, &consumer.job_id)
            .await
            .map_err(|error| error.to_string())?;
        if home.as_deref() == Some(home_uri) {
            own.push(consumer);
        }
    }
    if own.is_empty() {
        return Ok(Vec::new());
    }
    let registry = FactRegistry::default();
    let needs_content = registry.source(&row.source).is_some_and(|spec| {
        clauses_need_hydration(spec, own.iter().map(|consumer| &consumer.clause))
    });
    let fact = if needs_content {
        match hydrate(orch, row).await {
            Ok(Hydrated { fact, .. }) => fact,
            Err(_) => stored_fact(row)?,
        }
    } else {
        stored_fact(row)?
    };
    let actors = match registry.source(&row.source) {
        Some(spec) => actor_jobs(orch, row, spec, &stored_fact(row)?.fields).await,
        None => row.actor_job_id.iter().cloned().collect(),
    };
    let candidate = Fact {
        source: &fact.source,
        fields: &fact.fields,
        presence: super::log::presence_from_str(&row.presence),
    };
    Ok(own
        .into_iter()
        .map(|consumer| {
            let mut reasons = super::explain_clause(
                &consumer.clause,
                &candidate,
                Some(&consumer.scope(row.project_id.as_deref())),
            );
            if actors.contains(&consumer.job_id) {
                reasons.insert(
                    0,
                    "the row is the thread's own write, which never wakes it".into(),
                );
            }
            ClauseVerdict {
                clause_ref: consumer.clause_ref.clone(),
                reasons,
            }
        })
        .collect())
}

/// The addressee field through which `row` names the home at `home_uri`, if
/// it names it at all. Such a home is woken by the row itself, whatever its
/// standing triggers say, subject to the same actor and jurisdiction rules.
pub(crate) async fn addressed_via(
    orch: &Orchestrator,
    row: &FactRow,
    home_uri: &str,
) -> Result<Option<&'static str>, String> {
    let registry = FactRegistry::default();
    let Some(spec) = registry.source(&row.source) else {
        return Ok(None);
    };
    let stored = stored_fact(row)?;
    for field in spec.addressees {
        for uri in addressee_uris(&stored.fields, field) {
            for job in crate::orchestrator::wakes::addressed_jobs(&orch.db.local, &uri).await? {
                let home = crate::jobs::queries::home_uri_for_job(&orch.db.local, &job)
                    .await
                    .map_err(|error| error.to_string())?;
                if home.as_deref() == Some(home_uri) {
                    return Ok(Some(field));
                }
            }
        }
    }
    Ok(None)
}

/// Dispatch one claimed row; returns the state and note it settles in.
async fn run_route_row(
    orch: &Orchestrator,
    row: FactRow,
    timeout: Duration,
) -> (&'static str, Option<String>) {
    let hydrated = match hydrate(orch, &row).await {
        Ok(hydrated) => hydrated,
        Err(error) => return (route_state::FAILED, Some(format!("hydrate: {error}"))),
    };
    let path = project_path(orch, row.project_id.as_deref()).await;
    let routes = match orch
        .fact_log
        .index
        .routes(&orch.config_dir, path.as_deref())
    {
        Ok(routes) => routes,
        Err(error) => return (route_state::FAILED, Some(format!("routes: {error}"))),
    };
    // The submissions live outside the timed future: a timeout cancels the
    // route that hung, not the deliveries the routes before it prepared.
    let mut submissions = Vec::new();
    let result = tokio::time::timeout(
        timeout,
        super::dispatcher::dispatch_row(
            orch,
            &hydrated.fact,
            hydrated.presence,
            RouteContext {
                project_id: row.project_id.as_deref(),
                project_path: path.as_deref(),
            },
            &routes,
            Some(row.id),
            &mut submissions,
        ),
    )
    .await
    .unwrap_or_else(|_| Err("timeout".to_string()));
    let handover = hand_over(orch, &hydrated, submissions).await;
    match (result, handover) {
        (Ok(()), Ok(())) => (route_state::DONE, None),
        (Err(error), _) | (Ok(()), Err(error)) => (route_state::FAILED, Some(error)),
    }
}

/// Give each channel submission to the routers that should deliver it.
///
/// A sink addressed to one conversation reaches that conversation's provider
/// (the channel runtime filters on the destination). An undirected sink
/// reaches the source's audience: every provider, or only the providers that
/// were following the fact's thread when it was said. Each provider's router
/// gates, defers, delivers, and journals its own copy.
async fn hand_over(
    orch: &Orchestrator,
    hydrated: &Hydrated,
    submissions: Vec<super::ChannelSubmission>,
) -> Result<(), String> {
    let registry = FactRegistry::default();
    let audience = registry
        .source(&hydrated.fact.source)
        .map_or(Audience::AllProviders, |spec| spec.audience);
    for submission in submissions {
        let recipients = match (&submission.destination, audience) {
            (Some(_), _) | (None, Audience::AllProviders) => vec![submission],
            (None, Audience::FollowersOf(field)) => {
                let target = hydrated
                    .fact
                    .fields
                    .get(field)
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let followers = followers_at(orch, target, hydrated.occurred_at_ms).await?;
                if followers.is_empty() {
                    super::dispatcher::record_channel_drop(orch, &submission, "no_follower")
                        .await?;
                    continue;
                }
                followers
                    .into_iter()
                    .map(|provider| {
                        let mut addressed = submission.clone();
                        addressed.provider = Some(provider);
                        addressed
                    })
                    .collect()
            }
        };
        for submission in recipients {
            if let Err(submission) = crate::channels::submit_route(submission) {
                super::record_channel_outcome(
                    orch,
                    &submission,
                    Err("channel runtime is unavailable".into()),
                )
                .await?;
            }
        }
    }
    Ok(())
}

/// Providers following `target` whose follow began no later than the fact.
/// A follow starts at the thread's live edge, so an utterance said before it
/// is not that provider's to receive. The transcript dates an utterance to the
/// second, so the comparison is at second granularity.
async fn followers_at(
    orch: &Orchestrator,
    target: &str,
    occurred_at_ms: Option<i64>,
) -> Result<Vec<String>, String> {
    let follows = crate::channels::ledger::list_follow_providers(&orch.db.local, target).await?;
    Ok(follows
        .into_iter()
        .filter(|(_, followed_at)| {
            occurred_at_ms.is_none_or(|occurred| followed_at / 1000 <= occurred / 1000)
        })
        .map(|(provider, _)| provider)
        .collect())
}

async fn prune_pass(orch: &Orchestrator, policy: &EventLogPolicy) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp_millis();
    let mut selectors: Vec<(crate::storage::PruneSelector, i64)> = policy
        .retention_ms
        .iter()
        .map(|(source, ms)| (crate::storage::PruneSelector::Source(source.clone()), *ms))
        .collect();
    selectors.push((
        crate::storage::PruneSelector::Except(policy.retention_ms.keys().cloned().collect()),
        policy.default_retention_ms,
    ));
    for (selector, retention) in selectors {
        let cutoff = now.saturating_sub(retention);
        while crate::storage::prune_facts(&orch.db.local, &selector, cutoff, PRUNE_BATCH)
            .await
            .map_err(|error| error.to_string())?
            == PRUNE_BATCH as u64
        {
            tokio::task::yield_now().await;
        }
        while crate::storage::prune_fact_wakes(&orch.db.local, &selector, cutoff, PRUNE_BATCH)
            .await
            .map_err(|error| error.to_string())?
            == PRUNE_BATCH as u64
        {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

/// Run both lanes until neither has work left, delivering every pending wake
/// as though its window had closed. Tests mint, then drain.
#[cfg(test)]
pub(crate) async fn drain_fact_log(orch: &Orchestrator) {
    drain_fact_log_within(orch, EventLogPolicy::default().dispatch_timeout).await;
}

/// [`drain_fact_log`] with the route lane bounded by `dispatch_timeout`.
#[cfg(test)]
pub(crate) async fn drain_fact_log_within(orch: &Orchestrator, dispatch_timeout: Duration) {
    let policy = EventLogPolicy {
        dispatch_timeout,
        ..EventLogPolicy::default()
    };
    loop {
        let indexed = index_pass(orch, &policy).await.expect("index pass");
        let routed = route_pass(orch, &policy).await.expect("route pass");
        let flushed = flush_due_wakes(orch, i64::MAX).await.expect("wake flush");
        if indexed == 0 && routed == 0 && flushed == 0 {
            return;
        }
    }
}

/// A project with an issue node that writes files and a thread that watches.
#[cfg(test)]
pub(crate) mod fixture {
    use std::path::Path;
    use std::sync::Arc;

    use crate::db::DbState;
    use crate::orchestrator::Orchestrator;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, LocalDb, SearchIndex};

    pub(crate) const SQL: &str = "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','Cairn','cairn','/tmp/cairn',1,1);
         INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',42,'Target','active',1,1);
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('e','build','i','p','running',1,1);
         INSERT INTO jobs(id,execution_id,issue_id,project_id,status,uri_segment,node_name,created_at,updated_at) VALUES('writer','e','i','p','running','builder','builder',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('t','p','ui','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('watcher','t','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('watcher-run','watcher','running',1,1);";

    pub(crate) async fn orchestrator(
        temp: &Path,
        routes: &[(&str, &str)],
    ) -> (Orchestrator, Arc<LocalDb>) {
        let config = temp.join("config");
        // A bare config dir: no pack sync runs here, so the only routes are
        // the ones each test declares.
        std::fs::create_dir_all(config.join("routes")).unwrap();
        for (name, body) in routes {
            std::fs::write(config.join(format!("routes/{name}.yaml")), body).unwrap();
        }
        let db = Arc::new(
            migrated_test_db(&format!(
                "fact-log-{}.db",
                temp.file_name().unwrap().to_string_lossy()
            ))
            .await,
        );
        db.execute_batch(SQL).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config,
        )
        .boot_at(0)
        .build();
        (orch, db)
    }

    /// Give `job` the thread standing triggers in `triggers` (JSON clauses).
    pub(crate) async fn standing_triggers(db: &LocalDb, job: &str, triggers: &str) {
        let triggers: Vec<crate::routes::TriggerClause> = serde_json::from_str(triggers).unwrap();
        let job = job.to_string();
        db.write(move |conn| {
            let triggers = triggers.clone();
            let job = job.clone();
            Box::pin(async move {
                crate::orchestrator::wakes::rebuild_derived_thread_subscriptions_conn(
                    conn, &job, &triggers,
                )
                .await
            })
        })
        .await
        .unwrap();
    }

    /// Record one agent write the way the write handler does and wait for its
    /// fact to land. Returns the `file_changes` id.
    pub(crate) async fn agent_write(
        orch: &Orchestrator,
        db: &Arc<LocalDb>,
        job: &str,
        path: &str,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        db.execute(
            "INSERT INTO file_changes(id,job_id,file_path,status,additions,deletions,previous_path,created_at) VALUES(?1,?2,?3,'modified',12,3,NULL,1)",
            (id.clone(), job.to_string(), path.to_string()),
        )
        .await
        .unwrap();
        crate::mcp::handlers::write::file_mutations::spawn_file_changed_mint(
            orch,
            db.clone(),
            crate::mcp::handlers::write::file_mutations::RecordedFileChange {
                id: id.clone(),
                job_id: job.to_string(),
                path: path.to_string(),
                previous_path: None,
                status: "modified".into(),
                additions: 12,
                deletions: 3,
            },
        );
        let identity = format!("file-change:{id}");
        for _ in 0..500 {
            if db
                .query_opt_i64(
                    "SELECT 1 FROM fact_log WHERE identity = ?1",
                    (identity.clone(),),
                )
                .await
                .unwrap()
                .is_some()
            {
                return id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the file_changed fact for {path} was never minted");
    }

    use std::time::Duration;
}

#[cfg(test)]
#[path = "worker_posts_tests.rs"]
mod posts_tests;

#[cfg(test)]
mod tests {
    use super::fixture::*;
    use super::*;
    use crate::routes::{Mint, Presence, RouteFact};
    use std::collections::BTreeMap;

    fn github_fact(comment: u64) -> RouteFact {
        RouteFact {
            source: "github_comment".into(),
            identity: format!("github-comment:{comment}"),
            fields: BTreeMap::from([
                ("project".into(), serde_json::json!("cairn")),
                (
                    "url".into(),
                    serde_json::json!(format!(
                        "https://github.com/o/r/issues/1#issuecomment-{comment}"
                    )),
                ),
                ("title".into(), serde_json::json!("GitHub mention")),
                ("text".into(), serde_json::json!("@cairn please look")),
            ]),
            origin: None,
            summary: Some(format!("GitHub mention {comment}")),
            route_provenance: None,
        }
    }

    async fn mint_github(orch: &Orchestrator, comment: u64) -> Option<i64> {
        crate::routes::mint(
            orch,
            Mint {
                record_ref: format!("https://github.com/o/r/issues/1#issuecomment-{comment}"),
                record_id: Some(comment.to_string()),
                fact: github_fact(comment),
                project_id: Some("p"),
                presence: Presence::Away,
                actor_job_id: None,
            },
        )
        .await
        .unwrap()
    }

    async fn row(db: &crate::storage::LocalDb, id: i64) -> FactRow {
        crate::storage::get_fact(db, id).await.unwrap().unwrap()
    }

    const MESSAGE_ROUTE: &str = "name: Mentions to the issue\ndescription: test\nnodes:\n  - id: in\n    type: trigger\n    when: { fact: github_comment }\n  - id: out\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/42' }\nedges:\n  - { from: in, to: out }\n";

    #[tokio::test]
    async fn an_unmatched_fact_still_has_a_readable_row_whose_ref_resolves() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        let id = mint_github(&orch, 77).await.unwrap();
        drain_fact_log(&orch).await;

        let stored = row(&db, id).await;
        assert_eq!(stored.route_state, route_state::UNMATCHED);
        assert_eq!(stored.record_id.as_deref(), Some("77"));

        let list = crate::resources::events::list(&orch, Some("cairn"), &[]).await;
        assert!(
            list.contains(&format!("[#{id}](cairn://p/cairn/events/{id})")),
            "{list}"
        );
        assert!(list.contains("routes: unmatched"), "{list}");
        assert!(
            list.contains("record: https://github.com/o/r/issues/1#issuecomment-77"),
            "{list}"
        );
        let member = crate::resources::events::member(&orch, Some("cairn"), id, &[]).await;
        assert!(member.contains("- GitHub comment id: 77"), "{member}");
        assert!(
            member.contains("- ref: https://github.com/o/r/issues/1#issuecomment-77"),
            "{member}"
        );
    }

    #[tokio::test]
    async fn a_matched_fact_joins_its_firing_to_its_row() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[("to-issue", MESSAGE_ROUTE)]).await;
        let id = mint_github(&orch, 5).await.unwrap();
        drain_fact_log(&orch).await;

        assert_eq!(row(&db, id).await.route_state, route_state::DONE);
        let firings = crate::storage::list_firings_for_fact(&db, id)
            .await
            .unwrap();
        let firing = firings
            .iter()
            .find(|firing| firing.route_id == "to-issue")
            .expect("the route's firing joins the row");
        assert_eq!(firing.status, "fired");
        let list = crate::resources::events::list(&orch, Some("cairn"), &[]).await;
        assert!(
            list.contains(&format!(
                "[to-issue #{}](cairn://routes/to-issue/history/{})",
                firing.seq, firing.seq
            )),
            "{list}"
        );
    }

    #[tokio::test]
    async fn a_file_changed_path_prefix_wakes_the_thread_only_under_its_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        standing_triggers(
            &db,
            "watcher",
            r#"[{"fact":"file_changed","pathPrefix":"src/a/"}]"#,
        )
        .await;
        assert_eq!(
            db.query_text(
                "SELECT source_kind || ':' || fact_kinds_json FROM wake_subscriptions WHERE job_id = 'watcher' AND id LIKE 'derived:thread:%'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some(r#"fact:["file_changed"]"#),
            "the trigger compiles to an ordinary derived wake subscription"
        );

        let inside = agent_write(&orch, &db, "writer", "src/a/x.rs").await;
        let outside = agent_write(&orch, &db, "writer", "src/b/x.rs").await;
        drain_fact_log(&orch).await;

        let wakes = |file: String| {
            let db = db.clone();
            async move {
                db.query_opt_text(
                    "SELECT wakes_json FROM fact_log WHERE identity = ?1",
                    (format!("file-change:{file}"),),
                )
                .await
                .unwrap()
            }
        };
        let woke: Vec<Value> = serde_json::from_str(&wakes(inside).await.unwrap()).unwrap();
        assert_eq!(woke.len(), 1);
        assert_eq!(
            (woke[0]["job"].as_str(), woke[0]["action"].as_str()),
            (Some("watcher"), Some("coalesced"))
        );
        assert!(woke[0]["wake"].is_i64(), "{woke:?}");
        assert_eq!(
            wakes(outside).await,
            None,
            "a write outside the prefix wakes nothing"
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM messages WHERE recipient_run_id = 'watcher-run' AND content LIKE '[Event] src/a/x.rs modified%'",
                (),
            )
            .await
            .unwrap(),
            Some(1),
            "the wake reached the thread's session"
        );
        let list = crate::resources::events::list(&orch, Some("cairn"), &[]).await;
        assert!(list.contains("woke 1 thread: "), "{list}");
        assert!(list.contains("routes: unmatched"), "{list}");
        assert!(list.contains("by cairn://p/cairn/42/1/builder"), "{list}");
    }

    #[tokio::test]
    async fn a_thread_is_never_woken_by_its_own_write() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        standing_triggers(
            &db,
            "watcher",
            r#"[{"fact":"file_changed","pathPrefix":"src/"}]"#,
        )
        .await;
        let own = agent_write(&orch, &db, "watcher", "src/a/x.rs").await;
        drain_fact_log(&orch).await;
        assert_eq!(
            db.query_opt_text(
                "SELECT wakes_json FROM fact_log WHERE identity = ?1",
                (format!("file-change:{own}"),),
            )
            .await
            .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_file_changed_row_holds_pointers_and_registry_fields_only() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        let id = agent_write(&orch, &db, "writer", "src/a/x.rs").await;
        let stored = db
            .query_one(
                "SELECT fields_json, record_id, record_ref, actor_job_id FROM fact_log WHERE identity = ?1",
                (format!("file-change:{id}"),),
                |row| {
                    Ok((
                        crate::storage::RowExt::text(row, 0)?,
                        crate::storage::RowExt::text(row, 1)?,
                        crate::storage::RowExt::text(row, 2)?,
                        crate::storage::RowExt::text(row, 3)?,
                    ))
                },
            )
            .await
            .unwrap();
        let fields: BTreeMap<String, Value> = serde_json::from_str(&stored.0).unwrap();
        let registry = FactRegistry::default();
        let spec = registry.source("file_changed").unwrap();
        assert!(fields
            .keys()
            .all(|key| spec.fields.get(key.as_str()).is_some_and(|f| f.stored)));
        assert_eq!(
            fields.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "additions",
                "deletions",
                "node",
                "path",
                "project",
                "status"
            ]
        );
        assert_eq!(stored.1, id, "record_id is the file_changes row");
        assert_eq!(
            stored.2,
            "cairn://p/cairn/42/1/builder/changed?glob=src/a/x.rs"
        );
        assert_eq!(stored.3, "writer");
        assert_eq!(
            db.query_opt_i64("SELECT 1 FROM file_changes WHERE id = ?1", (stored.1,))
                .await
                .unwrap(),
            Some(1),
            "the pointer reaches the canonical row"
        );
    }

    /// The cache of thread consumers must follow an edit that keeps the same
    /// number of clauses, which a rebuild can store under the same rowids.
    #[tokio::test]
    async fn editing_a_standing_trigger_replaces_the_cached_clause() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        standing_triggers(
            &db,
            "watcher",
            r#"[{"fact":"file_changed","pathPrefix":"old/"}]"#,
        )
        .await;
        agent_write(&orch, &db, "writer", "old/x.rs").await;
        drain_fact_log(&orch).await;

        standing_triggers(
            &db,
            "watcher",
            r#"[{"fact":"file_changed","pathPrefix":"new/"}]"#,
        )
        .await;
        let moved = agent_write(&orch, &db, "writer", "new/x.rs").await;
        let replaced = agent_write(&orch, &db, "writer", "old/y.rs").await;
        drain_fact_log(&orch).await;

        let wakes = |file: String| {
            let db = db.clone();
            async move {
                db.query_opt_text(
                    "SELECT wakes_json FROM fact_log WHERE identity = ?1",
                    (format!("file-change:{file}"),),
                )
                .await
                .unwrap()
            }
        };
        assert!(
            wakes(moved)
                .await
                .is_some_and(|wakes| wakes.contains("coalesced")),
            "the edited clause is live"
        );
        assert_eq!(wakes(replaced).await, None, "the replaced clause is gone");
    }

    /// A relayed comment has no other local record, so its row is the whole
    /// comment: a long mention must reach the bundled route's issue intact.
    #[tokio::test]
    async fn a_long_mention_reaches_its_issue_whole_and_is_stored_once() {
        let temp = tempfile::tempdir().unwrap();
        let shipped = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../packs/core/routes/github-mention.yaml"),
        )
        .expect("the core pack ships the mention route");
        let (orch, db) = orchestrator(temp.path(), &[("github-mention", &shipped)]).await;
        let text = format!("@cairn {}MARKER-PAST-2000", "a".repeat(2500));
        let comment = crate::github::relay::CommentTrigger {
            comment_id: 88,
            repository: "o/r".into(),
            number: 1,
            kind: "issue",
            author: "someone".into(),
            url: "https://github.com/o/r/issues/1#issuecomment-88".into(),
            body: text.clone(),
        };
        let origin = crate::routes::installation_machine_origin(&orch).unwrap();
        let id = crate::routes::mint(
            &orch,
            Mint {
                fact: crate::github::relay::comment_fact(&comment, "cairn", origin),
                record_ref: comment.url.clone(),
                record_id: Some("88".into()),
                project_id: Some("p"),
                presence: Presence::Away,
                actor_job_id: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
        drain_fact_log(&orch).await;

        let stored: BTreeMap<String, Value> =
            serde_json::from_str(&row(&db, id).await.fields_json).unwrap();
        assert!(!stored.contains_key("body"), "the comment is stored once");
        assert_eq!(stored["text"], text);
        let description = db
            .query_opt_text(
                "SELECT description FROM issues WHERE id != 'i' ORDER BY created_at DESC LIMIT 1",
                (),
            )
            .await
            .unwrap()
            .expect("the bundled mention route created an issue");
        assert!(
            description.starts_with("Repository: o/r\n"),
            "{description}"
        );
        assert!(description.ends_with("MARKER-PAST-2000"), "{description}");
    }

    #[tokio::test]
    async fn route_yaml_is_read_when_it_changes_not_per_fact() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, _db) = orchestrator(temp.path(), &[("to-issue", MESSAGE_ROUTE)]).await;
        let reads = || crate::config::routes::ROUTE_DIR_READS.with(std::cell::Cell::get);
        let before = reads();
        for comment in 0..50 {
            mint_github(&orch, comment).await;
        }
        drain_fact_log(&orch).await;
        assert_eq!(reads() - before, 1, "50 facts, one directory read");

        crate::routes::invalidate_trigger_index(&orch);
        mint_github(&orch, 1000).await;
        drain_fact_log(&orch).await;
        assert_eq!(reads() - before, 2, "a route write costs exactly one more");
    }

    #[tokio::test]
    async fn a_hung_route_delays_neither_producers_nor_thread_wakes() {
        let temp = tempfile::tempdir().unwrap();
        let hang = format!(
            "name: Hang\ndescription: never answers\nnodes:\n  - id: in\n    type: trigger\n    when: {{ fact: github_comment }}\n  - id: wait\n    type: response\n    response: {}\n  - id: out\n    type: sink\n    sink: {{ kind: message, target: 'cairn://p/cairn/42' }}\nedges:\n  - {{ from: in, to: wait }}\n  - {{ from: wait, to: out }}\n",
            crate::routes::dispatcher::HANGING_RESPONSE
        );
        let (orch, db) = orchestrator(temp.path(), &[("hang", &hang)]).await;
        standing_triggers(
            &db,
            "watcher",
            r#"[{"fact":"file_changed","pathPrefix":"src/"}]"#,
        )
        .await;

        // The producer's whole obligation is one insert.
        let stuck = tokio::time::timeout(Duration::from_secs(1), mint_github(&orch, 1))
            .await
            .expect("minting never waits on a route")
            .unwrap();
        let policy = EventLogPolicy::default();
        index_pass(&orch, &policy).await.unwrap();
        assert_eq!(row(&db, stuck).await.route_state, route_state::QUEUED);

        let lane_orch = orch.clone();
        let lane = tokio::spawn(async move {
            let policy = EventLogPolicy {
                dispatch_timeout: Duration::from_secs(3600),
                ..EventLogPolicy::default()
            };
            route_pass(&lane_orch, &policy).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(row(&db, stuck).await.route_state, route_state::RUNNING);

        // While the route lane is stuck, a later row still wakes its thread.
        let later = tokio::time::timeout(Duration::from_secs(5), async {
            let id = agent_write(&orch, &db, "writer", "src/a/x.rs").await;
            index_pass(&orch, &policy).await.unwrap();
            assert_eq!(flush_due_wakes(&orch, i64::MAX).await.unwrap(), 1);
            id
        })
        .await
        .expect("the index lane does not wait for the route lane");
        assert!(db
            .query_opt_text(
                "SELECT wakes_json FROM fact_log WHERE identity = ?1",
                (format!("file-change:{later}"),),
            )
            .await
            .unwrap()
            .is_some_and(|wakes| wakes.contains("coalesced")));
        assert_eq!(
            db.query_opt_text("SELECT delivered_action FROM fact_log_wakes", ())
                .await
                .unwrap()
                .as_deref(),
            Some("delivered")
        );
        lane.abort();

        // The route lane's own bound gives up on a hung row.
        let bounded = mint_github(&orch, 2).await.unwrap();
        index_pass(&orch, &policy).await.unwrap();
        let policy = EventLogPolicy {
            dispatch_timeout: Duration::from_millis(100),
            ..EventLogPolicy::default()
        };
        route_pass(&orch, &policy).await.unwrap();
        let settled = row(&db, bounded).await;
        assert_eq!(
            (settled.route_state.as_str(), settled.route_note.as_deref()),
            (route_state::FAILED, Some("timeout"))
        );
    }

    /// A second project, key `other`, with two thread sessions:
    /// `other-watcher` and `other-writer`.
    async fn other_project_thread(db: &crate::storage::LocalDb) {
        db.execute_batch(
            "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('q','w','Other','other','/tmp/other',1,1);
             INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tq','q','ui','active',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('other-watcher','tq','q','running','thread',1,1);
             INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('other-watcher-run','other-watcher','running',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('other-writer','tq','q','running','thread',1,1);
             INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('other-writer-run','other-writer','running',1,1);",
        )
        .await
        .unwrap();
    }

    fn woken_jobs(wakes_json: Option<String>) -> Vec<String> {
        let wakes: Vec<Value> = wakes_json
            .map(|json| serde_json::from_str(&json).unwrap())
            .unwrap_or_default();
        let mut jobs: Vec<String> = wakes
            .iter()
            .map(|wake| {
                assert_eq!(wake["action"], "coalesced", "{wake}");
                assert!(wake["wake"].is_i64(), "{wake}");
                wake["job"].as_str().unwrap().to_owned()
            })
            .collect();
        jobs.sort();
        jobs
    }

    #[tokio::test]
    async fn a_project_scoped_row_wakes_only_threads_in_its_project() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        other_project_thread(&db).await;
        let clause = r#"[{"fact":"file_changed","pathPrefix":"src/"}]"#;
        standing_triggers(&db, "watcher", clause).await;
        standing_triggers(&db, "other-watcher", clause).await;

        let write = agent_write(&orch, &db, "writer", "src/a/x.rs").await;
        drain_fact_log(&orch).await;

        let wakes = db
            .query_opt_text(
                "SELECT wakes_json FROM fact_log WHERE identity = ?1",
                (format!("file-change:{write}"),),
            )
            .await
            .unwrap();
        assert_eq!(woken_jobs(wakes), vec!["watcher"]);
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM messages WHERE recipient_run_id = 'other-watcher-run'",
                (),
            )
            .await
            .unwrap(),
            Some(0),
            "the other project's thread is never woken"
        );

        // The consumer's project comes from the index query's join, so the
        // check per row is a comparison.
        let consumers = orch.fact_log.index.threads(&db).await.unwrap();
        let mut projects: Vec<(&str, Option<&str>, Option<&str>)> = consumers["file_changed"]
            .iter()
            .map(|consumer| {
                (
                    consumer.job_id.as_str(),
                    consumer.project_id.as_deref(),
                    consumer.project_key.as_deref(),
                )
            })
            .collect();
        projects.sort();
        assert_eq!(
            projects,
            vec![
                ("other-watcher", Some("q"), Some("other")),
                ("watcher", Some("p"), Some("cairn"))
            ]
        );
    }

    async fn file_change_wakes(db: &crate::storage::LocalDb, change: &str) -> Vec<String> {
        woken_jobs(
            db.query_opt_text(
                "SELECT wakes_json FROM fact_log WHERE identity = ?1",
                (format!("file-change:{change}"),),
            )
            .await
            .unwrap(),
        )
    }

    /// Naming `project` replaces the own-project default rather than adding to
    /// it: the A thread watching B hears B and no longer hears A.
    #[tokio::test]
    async fn a_clause_naming_another_project_replaces_the_own_project_scope() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        other_project_thread(&db).await;
        standing_triggers(
            &db,
            "watcher",
            r#"[{"fact":"file_changed","project":"other","pathPrefix":"src/"}]"#,
        )
        .await;

        let in_other = agent_write(&orch, &db, "other-writer", "src/b.rs").await;
        let in_own = agent_write(&orch, &db, "writer", "src/a.rs").await;
        drain_fact_log(&orch).await;

        assert_eq!(file_change_wakes(&db, &in_other).await, vec!["watcher"]);
        assert!(
            file_change_wakes(&db, &in_own).await.is_empty(),
            "an explicit project replaces the default scope"
        );
    }

    /// The thread-scoped explanation is reachable: an event read with
    /// `?explain=<thread>` names which scope kept the row out, or that it wakes.
    #[tokio::test]
    async fn an_event_explains_which_scope_kept_it_from_a_thread() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        other_project_thread(&db).await;
        standing_triggers(
            &db,
            "watcher",
            r#"[{"fact":"file_changed","pathPrefix":"src/"}]"#,
        )
        .await;
        standing_triggers(
            &db,
            "other-watcher",
            r#"[{"fact":"file_changed","project":"third","pathPrefix":"src/"}]"#,
        )
        .await;
        let in_own = agent_write(&orch, &db, "writer", "src/a.rs").await;
        let in_other = agent_write(&orch, &db, "other-writer", "src/b.rs").await;
        drain_fact_log(&orch).await;

        let event = |change: String| {
            let db = db.clone();
            async move {
                db.query_opt_i64(
                    "SELECT id FROM fact_log WHERE identity = ?1",
                    (format!("file-change:{change}"),),
                )
                .await
                .unwrap()
                .unwrap()
            }
        };
        let home = |job: &'static str| {
            let db = db.clone();
            async move {
                crate::jobs::queries::home_uri_for_job(&db, job)
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        // Through the real read path from a URI string, so the query grammar
        // (which splits `&` only before a known key) is part of what is tested.
        let read = |uri: String| {
            let orch = orch.clone();
            async move {
                let request = crate::mcp::types::McpCallbackRequest {
                    thread_id: None,
                    cwd: "/tmp".to_string(),
                    run_id: None,
                    tool: "read".to_string(),
                    payload: serde_json::json!({}),
                    tool_use_id: None,
                };
                crate::resources::read_cairn_resource(&orch, &request, &uri).await
            }
        };
        let explain = |project: &'static str, id: i64, thread: String| {
            read(format!("cairn://p/{project}/events/{id}?explain={thread}"))
        };
        let watcher = home("watcher").await;
        let other_watcher = home("other-watcher").await;
        let own_row = event(in_own).await;
        let other_row = event(in_other).await;

        let woke = explain("cairn", own_row, watcher.clone()).await;
        assert!(
            woke.contains(&format!("## Standing triggers of {watcher}")),
            "{woke}"
        );
        assert!(woke.contains("- would wake: `"), "{woke}");

        let by_default = explain("other", other_row, watcher.clone()).await;
        assert!(by_default.contains("- would not wake: `"), "{by_default}");
        assert!(
            by_default.contains("the clause names none, so the default scope applies (this thread's own project cairn plus workspace events); the row is in project other"),
            "{by_default}"
        );

        // A grep that matches nothing echoes its own pattern, so the assertion
        // looks for text only the explanation itself renders.
        let grep_first = read(format!(
            "cairn://p/other/events/{other_row}?grep=default scope&explain={watcher}"
        ))
        .await;
        assert!(
            grep_first.contains("own project cairn plus workspace events"),
            "explain after a grep still reaches the event: {grep_first}"
        );

        let by_name = explain("other", other_row, other_watcher.clone()).await;
        assert!(
            by_name.contains(r#"project: the clause names "third", which replaces the own-project default; the row is in project other"#),
            "{by_name}"
        );
        assert!(
            !by_name.contains("the row is the thread's own write"),
            "{by_name}"
        );

        let none = explain("cairn", own_row, "cairn://p/cairn/42".into()).await;
        assert!(
            none.contains("cairn://p/cairn/42 has no standing trigger on file_changed."),
            "{none}"
        );
        let plain = crate::resources::events::member(&orch, Some("cairn"), own_row, &[]).await;
        assert!(!plain.contains("## Standing triggers"), "{plain}");
    }

    #[tokio::test]
    async fn a_clause_naming_a_list_of_projects_hears_each_of_them() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        other_project_thread(&db).await;
        standing_triggers(
            &db,
            "watcher",
            r#"[{"fact":"file_changed","project":["cairn","other"],"pathPrefix":"src/"}]"#,
        )
        .await;

        let in_other = agent_write(&orch, &db, "other-writer", "src/b.rs").await;
        let in_own = agent_write(&orch, &db, "writer", "src/a.rs").await;
        drain_fact_log(&orch).await;

        assert_eq!(file_change_wakes(&db, &in_other).await, vec!["watcher"]);
        assert_eq!(file_change_wakes(&db, &in_own).await, vec!["watcher"]);
    }

    /// A project-less row reaches every consumer on the default scope, in any
    /// project, and no consumer that names projects, even when the row's own
    /// `project` field would pass that test.
    #[tokio::test]
    async fn a_project_less_row_wakes_default_scope_threads_in_every_project_only() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        other_project_thread(&db).await;
        let clause = r#"[{"fact":"github_comment"}]"#;
        standing_triggers(&db, "watcher", clause).await;
        standing_triggers(&db, "other-watcher", clause).await;
        standing_triggers(
            &db,
            "other-writer",
            r#"[{"fact":"github_comment","project":"cairn"}]"#,
        )
        .await;

        let id = crate::routes::mint(
            &orch,
            Mint {
                fact: github_fact(9),
                record_ref: "https://github.com/o/r/issues/1#issuecomment-9".into(),
                record_id: Some("9".into()),
                project_id: None,
                presence: Presence::Away,
                actor_job_id: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
        drain_fact_log(&orch).await;

        assert_eq!(
            woken_jobs(row(&db, id).await.wakes_json),
            vec!["other-watcher", "watcher"]
        );
    }

    #[tokio::test]
    async fn a_project_less_fact_is_a_workspace_event_only() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, _db) = orchestrator(temp.path(), &[]).await;
        let id = crate::routes::mint(
            &orch,
            Mint {
                fact: github_fact(9),
                record_ref: "https://github.com/o/r/issues/1#issuecomment-9".into(),
                record_id: Some("9".into()),
                project_id: None,
                presence: Presence::Away,
                actor_job_id: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
        drain_fact_log(&orch).await;
        let workspace = crate::resources::events::list(&orch, None, &[]).await;
        assert!(
            workspace.contains(&format!("[#{id}](cairn://routes/events/{id})")),
            "{workspace}"
        );
        let project = crate::resources::events::list(&orch, Some("cairn"), &[]).await;
        assert!(project.contains("No events match."), "{project}");
        assert!(
            crate::resources::events::member(&orch, Some("cairn"), id, &[])
                .await
                .starts_with("Event not found"),
            "a project view never shows another scope's row"
        );
    }

    #[test]
    fn policy_defaults_and_overrides_come_from_event_log_settings() {
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(EventLogPolicy::load(temp.path()), EventLogPolicy::default());
        std::fs::write(
            temp.path().join("settings.yaml"),
            "eventLog:\n  retention: { default: 7d, attention: 2d, file_changed: nonsense }\n  dispatchTimeout: 5s\n",
        )
        .unwrap();
        let policy = EventLogPolicy::load(temp.path());
        assert_eq!(policy.default_retention_ms, 7 * DAY_MS);
        assert_eq!(policy.retention_ms["attention"], 2 * DAY_MS);
        assert_eq!(
            policy.retention_ms["file_changed"],
            14 * DAY_MS,
            "an unparseable value keeps the default"
        );
        assert_eq!(policy.dispatch_timeout, Duration::from_secs(5));
        assert_eq!(
            (
                policy.wake_window("github_comment"),
                policy.wake_window("file_changed")
            ),
            (
                WakeWindow {
                    settle_ms: 30_000,
                    max_window_ms: 300_000
                },
                WakeWindow {
                    settle_ms: 60_000,
                    max_window_ms: 300_000
                }
            ),
            "wake windows default to 30s (60s for file changes) under a 5m cap"
        );
        std::fs::write(
            temp.path().join("settings.yaml"),
            "eventLog:\n  wakeSettle: { default: 10s, attention: 2s }\n  wakeMaxWindow: 1m\n",
        )
        .unwrap();
        let policy = EventLogPolicy::load(temp.path());
        assert_eq!(policy.wake_window("attention").settle_ms, 2_000);
        assert_eq!(policy.wake_window("github_comment").settle_ms, 10_000);
        assert_eq!(policy.wake_window("file_changed").settle_ms, 60_000);
        assert_eq!(policy.wake_max_window_ms, 60_000);
    }

    /// Coalesced thread wakes, driven on synthetic mint times so the settle
    /// window and cap are exact.
    mod coalescing {
        use super::*;

        /// Mint time of the first row; any value far from 0 works.
        const T0: i64 = 1_000_000_000;
        const SECOND: i64 = 1000;

        /// A second thread, `watcher2`, beside the fixture's `watcher`.
        async fn second_thread(db: &crate::storage::LocalDb) {
            db.execute_batch(
                "INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('t2','p','other','active',1,1);
                 INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('watcher2','t2','p','running','thread',1,1);
                 INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('watcher2-run','watcher2','running',1,1);",
            )
            .await
            .unwrap();
        }

        /// Insert one `github_comment` row minted at `at`, by `actor`.
        async fn row_at(db: &crate::storage::LocalDb, n: i64, at: i64, actor: Option<&str>) -> i64 {
            crate::storage::insert_fact(
                db,
                crate::storage::NewFact {
                    project_id: Some("p".into()),
                    source: "github_comment".into(),
                    identity: format!("github-comment:{n}"),
                    record_ref: format!("https://github.com/o/r/issues/1#issuecomment-{n}"),
                    record_id: Some(n.to_string()),
                    fields_json: serde_json::json!({"project": "cairn", "title": "GitHub mention"})
                        .to_string(),
                    summary: Some(format!("GitHub mention {n}")),
                    presence: "away".into(),
                    actor_job_id: actor.map(str::to_owned),
                    origin_principal_json: None,
                    origin_appearance_json: None,
                    route_provenance: None,
                    minted_at: at,
                },
                crate::storage::LogDedupe::Unique,
            )
            .await
            .unwrap()
            .unwrap()
        }

        /// The `[Event]` messages a thread's session received, oldest first.
        async fn events_received(db: &crate::storage::LocalDb, run: &str) -> Vec<String> {
            db.query_all(
                "SELECT content FROM messages WHERE recipient_run_id = ?1 AND content LIKE '[Event]%' ORDER BY rowid",
                (run.to_string(),),
                |row| crate::storage::RowExt::text(row, 0),
            )
            .await
            .unwrap()
        }

        async fn wake_rows(db: &crate::storage::LocalDb) -> Vec<(String, i64, i64, i64)> {
            db.query_all(
                "SELECT job_id, count, first_row_id, last_row_id FROM fact_log_wakes ORDER BY id",
                (),
                |row| {
                    Ok((
                        crate::storage::RowExt::text(row, 0)?,
                        crate::storage::RowExt::i64(row, 1)?,
                        crate::storage::RowExt::i64(row, 2)?,
                        crate::storage::RowExt::i64(row, 3)?,
                    ))
                },
            )
            .await
            .unwrap()
        }

        fn query(pairs: &[(&str, String)]) -> Vec<cairn_common::query::QueryParam> {
            pairs
                .iter()
                .map(|(key, value)| cairn_common::query::QueryParam {
                    key: (*key).into(),
                    value: value.clone(),
                })
                .collect()
        }

        #[tokio::test]
        async fn a_burst_of_twenty_rows_is_one_wake_carrying_its_count_and_range() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            standing_triggers(&db, "watcher", r#"[{"fact":"github_comment"}]"#).await;
            let policy = EventLogPolicy::default();
            let mut ids = Vec::new();
            for n in 0..20 {
                ids.push(row_at(&db, n, T0 + n * SECOND, None).await);
                index_pass(&orch, &policy).await.unwrap();
                flush_due_wakes(&orch, T0 + n * SECOND).await.unwrap();
            }
            let (first, last) = (ids[0], ids[19]);
            let quiet_from = T0 + 19 * SECOND;

            let list = crate::resources::events::list(&orch, Some("cairn"), &[]).await;
            assert!(
                list.contains("woke 0 threads, 1 pending: ")
                    && list.contains("(coalesced, pending: 20 rows so far)"),
                "{list}"
            );
            assert_eq!(
                flush_due_wakes(&orch, quiet_from + 30 * SECOND - 1)
                    .await
                    .unwrap(),
                0,
                "the wake waits out the settle window after the last row"
            );
            assert_eq!(
                flush_due_wakes(&orch, quiet_from + 30 * SECOND)
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(flush_due_wakes(&orch, i64::MAX).await.unwrap(), 0);

            assert_eq!(
                wake_rows(&db).await,
                vec![("watcher".to_string(), 20, first, last)]
            );
            assert_eq!(
                events_received(&db, "watcher-run").await,
                vec![format!(
                    "[Event] 20 github_comment rows in cairn (ids {first}\u{2013}{last}): cairn://p/cairn/events?source=github_comment&from={first}&to={last}"
                )],
                "the thread's session receives the batch once"
            );

            let list = crate::resources::events::list(&orch, Some("cairn"), &[]).await;
            assert_eq!(
                list.matches("(coalesced, 20 rows)").count(),
                20,
                "every coalesced row shows the delivered batch: {list}"
            );
            assert!(list.contains("woke 1 thread: "), "{list}");
            let member = crate::resources::events::member(&orch, Some("cairn"), ids[7], &[]).await;
            assert!(
                member.contains(&format!(
                    "coalesced into wake #1 (20 rows, ids {first}\u{2013}{last}): delivered"
                )),
                "{member}"
            );

            let ranged = crate::resources::events::list(
                &orch,
                Some("cairn"),
                &query(&[
                    ("source", "github_comment".into()),
                    ("from", (first + 1).to_string()),
                    ("to", (last - 1).to_string()),
                ]),
            )
            .await;
            let listed: Vec<i64> = ids
                .iter()
                .copied()
                .filter(|id| ranged.contains(&format!("[#{id}](")))
                .collect();
            assert_eq!(listed, ids[1..19].to_vec(), "{ranged}");
        }

        #[tokio::test]
        async fn one_isolated_row_wakes_within_one_settle_window_with_its_own_message() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            standing_triggers(&db, "watcher", r#"[{"fact":"github_comment"}]"#).await;
            let policy = EventLogPolicy::default();
            let id = row_at(&db, 1, T0, None).await;
            index_pass(&orch, &policy).await.unwrap();
            assert_eq!(
                flush_due_wakes(&orch, T0 + 30 * SECOND - 1).await.unwrap(),
                0
            );
            assert_eq!(flush_due_wakes(&orch, T0 + 30 * SECOND).await.unwrap(), 1);
            assert_eq!(
                events_received(&db, "watcher-run").await,
                vec![format!(
                    "[Event] GitHub mention 1 (github_comment): cairn://p/cairn/events/{id}"
                )],
                "one event still reads as one event"
            );
            let list = crate::resources::events::list(&orch, Some("cairn"), &[]).await;
            assert!(list.contains("woke 1 thread: "), "{list}");
            assert!(!list.contains("coalesced, 1 row"), "{list}");
        }

        #[tokio::test]
        async fn a_stream_past_the_max_window_wakes_once_per_window_not_once_at_the_end() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            standing_triggers(&db, "watcher", r#"[{"fact":"github_comment"}]"#).await;
            let policy = EventLogPolicy::default();
            // A row every 20s, each inside the previous row's 30s settle, for
            // ten minutes: the debounce alone would never come due.
            let mut delivered_during_stream = Vec::new();
            for n in 0..=30 {
                let at = T0 + n * 20 * SECOND;
                row_at(&db, n, at, None).await;
                index_pass(&orch, &policy).await.unwrap();
                if flush_due_wakes(&orch, at).await.unwrap() > 0 {
                    delivered_during_stream.push((at - T0) / SECOND);
                }
            }
            assert_eq!(
                delivered_during_stream,
                vec![300, 600],
                "the stream delivers at each 5m cap"
            );
            flush_due_wakes(&orch, i64::MAX).await.unwrap();
            let counts: Vec<i64> = wake_rows(&db).await.iter().map(|w| w.1).collect();
            assert_eq!(counts, vec![15, 15, 1]);
            assert_eq!(events_received(&db, "watcher-run").await.len(), 3);
        }

        #[tokio::test]
        async fn a_pending_wake_is_delivered_after_a_restart_with_its_count_and_never_twice() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            second_thread(&db).await;
            for job in ["watcher", "watcher2"] {
                standing_triggers(&db, job, r#"[{"fact":"github_comment"}]"#).await;
            }
            let policy = EventLogPolicy::default();
            let mut ids = Vec::new();
            for n in 0..5 {
                ids.push(row_at(&db, n, T0 + n * SECOND, None).await);
            }
            index_pass(&orch, &policy).await.unwrap();
            // The runner stops after claiming watcher's wake for delivery and
            // before recording what became of it; watcher2's is still pending.
            let claimed = crate::storage::claim_due_fact_wake(&db, i64::MAX, T0 + 60 * SECOND)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(claimed.job_id, "watcher");

            let recovery = crate::storage::recover_fact_log(&db, T0 + 120 * SECOND, 0)
                .await
                .unwrap();
            assert_eq!(recovery.interrupted_wakes, 1);
            assert_eq!(flush_due_wakes(&orch, T0 + 120 * SECOND).await.unwrap(), 1);
            assert_eq!(flush_due_wakes(&orch, i64::MAX).await.unwrap(), 0);

            let (first, last) = (ids[0], ids[4]);
            assert_eq!(
                events_received(&db, "watcher2-run").await,
                vec![format!(
                    "[Event] 5 github_comment rows in cairn (ids {first}\u{2013}{last}): cairn://p/cairn/events?source=github_comment&from={first}&to={last}"
                )],
                "the pending wake survives the restart with its count and range"
            );
            assert!(
                events_received(&db, "watcher-run").await.is_empty(),
                "a wake caught mid-delivery is never re-sent"
            );
            let list = crate::resources::events::list(&orch, Some("cairn"), &[]).await;
            assert!(list.contains("(coalesced, 5 rows, interrupted)"), "{list}");
        }

        #[tokio::test]
        async fn the_actors_own_rows_never_wake_it_even_inside_a_batch() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            standing_triggers(&db, "watcher", r#"[{"fact":"github_comment"}]"#).await;
            let policy = EventLogPolicy::default();
            let mut own = Vec::new();
            for n in 0..6 {
                let actor = (n % 2 == 0).then_some("watcher");
                let id = row_at(&db, n, T0 + n * SECOND, actor).await;
                if actor.is_some() {
                    own.push(id);
                }
            }
            index_pass(&orch, &policy).await.unwrap();
            flush_due_wakes(&orch, i64::MAX).await.unwrap();
            let wakes = wake_rows(&db).await;
            assert_eq!(
                wakes.iter().map(|w| w.1).collect::<Vec<_>>(),
                vec![3],
                "only the three rows others wrote"
            );
            for id in own {
                assert_eq!(row(&db, id).await.wakes_json, None);
            }
            let received = events_received(&db, "watcher-run").await;
            assert_eq!(received.len(), 1);
            assert!(
                received[0].starts_with("[Event] 3 github_comment rows"),
                "{received:?}"
            );
        }

        #[tokio::test]
        async fn each_thread_gets_its_own_wake_and_two_matching_clauses_make_one() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            second_thread(&db).await;
            standing_triggers(
                &db,
                "watcher",
                r#"[{"fact":"github_comment"},{"fact":"github_comment","project":"cairn"}]"#,
            )
            .await;
            standing_triggers(&db, "watcher2", r#"[{"fact":"github_comment"}]"#).await;
            let policy = EventLogPolicy::default();
            for n in 0..4 {
                row_at(&db, n, T0 + n * SECOND, None).await;
            }
            index_pass(&orch, &policy).await.unwrap();
            flush_due_wakes(&orch, i64::MAX).await.unwrap();
            let wakes = wake_rows(&db).await;
            assert_eq!(
                wakes
                    .iter()
                    .map(|w| (w.0.as_str(), w.1))
                    .collect::<Vec<_>>(),
                vec![("watcher", 4), ("watcher2", 4)]
            );
            for run in ["watcher-run", "watcher2-run"] {
                let received = events_received(&db, run).await;
                assert_eq!(received.len(), 1, "{run}: {received:?}");
                assert!(received[0].starts_with("[Event] 4 github_comment rows"));
            }
        }
    }
}
