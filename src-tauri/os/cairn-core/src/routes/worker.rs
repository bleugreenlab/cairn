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
//! for (a post's mentions, a commented post's author, a camp, an issue). The
//! index lane hands them and the matching clause consumers to the one
//! recipient resolver (`cairn_attention::wakes::recipients`), which merges
//! every route by stable recipient (a thread, never one of its sessions) at
//! the highest level any route reached it at, keeping every reason.
//!
//! Each recipient a row is owed to gets one **delivery receipt** per row,
//! unique per (fact, recipient), and the row is folded into that recipient's
//! pending wake for the source (`fact_log_wakes`) in the same transaction.
//! Wakes are **coalesced**: the tick delivers a wake once its settle window
//! closes, so a 30-file commit is one wake carrying a count and an id range,
//! not 30. The session a wake lands on is resolved at delivery.
//!
//! Route dispatch is at-most-once: a row the runner was dispatching when it
//! stopped becomes `interrupted` on restart and is never re-fired. Indexing
//! and wake delivery are idempotent instead, because receipts are: a row
//! caught mid-index is indexed again, and a wake caught mid-delivery is
//! delivered again, into the same message if its receipts were already bound
//! to one.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;

use cairn_attention::push::Wake;
use cairn_attention::runtime::{DeliveryOutcome, DeliveryRequest};
use cairn_attention::wakes::{
    receipt_state, Admission, Candidate, DeferUntil, FactAudience, NewReceipt, Reason,
    ReceiptWrite, SourceRef, StableRecipient,
};

use super::log::{clauses_need_hydration, hydrate, stored_fact, Hydrated};
use super::{matches, Audience, Fact, FactRegistry, RouteContext};
use crate::models::DeliveryUrgency;
use crate::orchestrator::wakes::{
    Addressed, MutedDelivery, WakeSubscriptionState, ADDRESSED_FACT_REF_PREFIX,
    CAMP_WATCH_FACT_REF_PREFIX, SOURCE_KIND_FACT,
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
                if recovery.interrupted
                    + recovery.stale
                    + recovery.reindexed
                    + recovery.requeued_wakes
                    > 0 =>
            {
                log::info!(
                    "fact log recovered {} interrupted and {} stale rows, re-indexes {} rows and re-delivers {} wakes",
                    recovery.interrupted,
                    recovery.stale,
                    recovery.reindexed,
                    recovery.requeued_wakes
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
        Some(project) => cairn_uri::build_project_event_uri(project, id),
        None => cairn_uri::build_workspace_event_uri(id),
    }
}

/// The canonical URI of one fact log row, attributed to the row's project:
/// what dispatch binds as [`EVENT_FIELD`](super::EVENT_FIELD).
pub async fn event_uri(orch: &Orchestrator, row: &FactRow) -> String {
    fact_row_uri(project_key(orch, row).await.as_deref(), row.id)
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
    let routes =
        orch.fact_log
            .index
            .routes(&orch.config_dir, path.as_deref(), orch.services.clock.now())?;
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

    let actors = actor_recipients(orch, &row, spec, &stored.fields).await;
    let window = policy.wake_window(&row.source);
    let host: std::sync::Arc<dyn cairn_attention::runtime::RecipientHost> =
        std::sync::Arc::new(crate::attention_host::CoreRecipients);
    let mut recipients = cairn_attention::wakes::Audience::default();
    // The addressee field that first reached each recipient, and the field
    // that named each camp: what the recipient's wake reads as.
    let mut via: BTreeMap<StableRecipient, &'static str> = BTreeMap::new();
    let mut camp_fields: BTreeMap<String, &'static str> = BTreeMap::new();
    let mut unresolved = Vec::new();
    for field in spec.addressees {
        for uri in addressee_uris(&stored.fields, field) {
            let mut audience = fact_audience(&row, &actors);
            let db = match crate::orchestrator::wakes::addressed(&orch.db, &uri).await {
                Ok(Addressed::Issue { db, uri }) => {
                    audience.source = Some(SourceRef::issue(&uri));
                    db
                }
                Ok(Addressed::Camp { db, camp_id }) => {
                    camp_fields.entry(camp_id.clone()).or_insert(field);
                    audience.camps.push(camp_id);
                    db
                }
                Ok(Addressed::Home(recipient)) => {
                    audience.offered.push((
                        recipient,
                        Reason::Addressed {
                            field: field.to_string(),
                        },
                        Wake::Wake,
                    ));
                    orch.db.local.clone()
                }
                Ok(Addressed::Nothing) => continue,
                Err(error) => {
                    unresolved.push(format!("{field} {uri}: {error}"));
                    continue;
                }
            };
            match cairn_attention::wakes::resolve_recipients(host.clone(), &db, &audience).await {
                Ok(found) => absorb(&mut recipients, &mut via, found, Some(field)),
                Err(error) => unresolved.push(format!("{field} {uri}: {error}")),
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

    // Standing triggers: each consumer has already applied its own `project`
    // test (`ThreadConsumer::admits`), so its reason is not jurisdictional.
    let mut triggered = fact_audience(&row, &actors);
    for consumer in &threads {
        if !matches(std::slice::from_ref(&consumer.clause), &candidate) {
            continue;
        }
        let recipient =
            cairn_attention::wakes::stable_recipient_for_job(&orch.db.local, &consumer.job_id)
                .await?;
        triggered.offered.push((
            recipient,
            Reason::Trigger {
                consumer: consumer.clause_ref.clone(),
            },
            Wake::Wake,
        ));
    }
    if !triggered.offered.is_empty() {
        let found =
            cairn_attention::wakes::resolve_recipients(host.clone(), &orch.db.local, &triggered)
                .await?;
        absorb(&mut recipients, &mut via, found, None);
    }

    let muted = spec.muted;
    let mut wakes = Vec::new();
    for candidate in recipients.into_candidates() {
        let Some(candidate) = apply_clause_rows(orch, &row, muted, candidate).await else {
            continue;
        };
        let clause_ref = wake_clause(
            &row.source,
            &candidate,
            via.get(&candidate.recipient).copied(),
            &camp_fields,
        );
        let field = via.get(&candidate.recipient).copied();
        fold_wake(
            orch, &mut wakes, &row, &candidate, clause_ref, field, window,
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

/// The audience one row's routes share: its project for jurisdiction, and
/// its own actors excluded.
fn fact_audience(row: &FactRow, actors: &[StableRecipient]) -> FactAudience {
    let mut audience = FactAudience::new(&row.source, Wake::Wake, Wake::Wake);
    audience.project_id = row.project_id.clone();
    audience.exclude.recipients = actors.to_vec();
    audience.exclude.jurisdiction = true;
    audience
}

/// Merge one resolution into the row's recipients, remembering the addressee
/// field that first reached each.
fn absorb(
    recipients: &mut cairn_attention::wakes::Audience,
    via: &mut BTreeMap<StableRecipient, &'static str>,
    found: Vec<Candidate>,
    field: Option<&'static str>,
) {
    for candidate in found {
        if let Some(field) = field {
            via.entry(candidate.recipient.clone()).or_insert(field);
        }
        for reached in candidate.reasons {
            recipients.offer(candidate.recipient.clone(), reached.reason, reached.level);
        }
    }
}

/// The clause a trigger or addressee reason is a subscription of.
fn reason_clause(source: &str, reason: &Reason) -> Option<String> {
    match reason {
        Reason::Trigger { consumer } => Some(consumer.clone()),
        Reason::Addressed { field } => Some(format!("{ADDRESSED_FACT_REF_PREFIX}{source}.{field}")),
        _ => None,
    }
}

/// Apply the recipient's own persisted rows on a trigger or addressee clause:
/// a muted row lowers only its own reason (to passive when the source rides
/// along, away entirely otherwise), and an unsubscribed row removes it. Every
/// other reason stands. `None` when no reason is left.
async fn apply_clause_rows(
    orch: &Orchestrator,
    row: &FactRow,
    muted: MutedDelivery,
    mut candidate: Candidate,
) -> Option<Candidate> {
    let job = match &candidate.recipient {
        StableRecipient::Job(job) => Some(job.clone()),
        StableRecipient::Thread(thread_id) => {
            let thread_id = thread_id.clone();
            orch.db
                .local
                .read(move |conn| {
                    let thread_id = thread_id.clone();
                    Box::pin(async move {
                        cairn_attention::wakes::thread_session_conn(conn, &thread_id).await
                    })
                })
                .await
                .ok()
                .flatten()
        }
    };
    if let Some(job) = job {
        let mut kept = Vec::with_capacity(candidate.reasons.len());
        for mut reached in std::mem::take(&mut candidate.reasons) {
            let Some(clause) = reason_clause(&row.source, &reached.reason) else {
                kept.push(reached);
                continue;
            };
            let state = crate::orchestrator::wakes::matching_subscription(
                &orch.db.local,
                &job,
                SOURCE_KIND_FACT,
                Some(&clause),
                &row.source,
            )
            .await
            .ok()
            .flatten()
            .map(|subscription| subscription.state);
            match state {
                Some(WakeSubscriptionState::Unsubscribed) => continue,
                Some(WakeSubscriptionState::Muted) if muted == MutedDelivery::Drop => continue,
                Some(WakeSubscriptionState::Muted) => reached.level = Wake::Passive,
                _ => {}
            }
            kept.push(reached);
        }
        candidate.reasons = kept;
        candidate.level = candidate
            .reasons
            .iter()
            .map(|reached| reached.level)
            .max()
            .unwrap_or(Wake::Passive);
    }
    (!candidate.reasons.is_empty()).then_some(candidate)
}

/// The clause a recipient's wake reads as: its loudest reason, an addressee
/// field before a camp role before a subscription or the parent edge before a
/// trigger on a tie, so the delivered line names the most specific route.
fn wake_clause(
    source: &str,
    candidate: &Candidate,
    field: Option<&str>,
    camp_fields: &BTreeMap<String, &'static str>,
) -> String {
    let rank = |reason: &Reason| match reason {
        Reason::Addressed { .. } => 0,
        Reason::CampLead { .. } => 1,
        Reason::CampRole { .. } => 2,
        Reason::Subscription { .. } => 3,
        Reason::Parent { .. } => 4,
        Reason::Trigger { .. } => 5,
        Reason::Backfilled { .. } => 6,
    };
    let loudest = candidate.reasons.iter().max_by(|a, b| {
        a.level
            .cmp(&b.level)
            .then(rank(&b.reason).cmp(&rank(&a.reason)))
    });
    let field = field.unwrap_or("addressee");
    let camp_field = |camp: &str| camp_fields.get(camp).copied().unwrap_or(field);
    match loudest.map(|reached| &reached.reason) {
        Some(Reason::Trigger { consumer }) => consumer.clone(),
        Some(Reason::Addressed { field }) => format!("{ADDRESSED_FACT_REF_PREFIX}{source}.{field}"),
        Some(Reason::CampLead { camp }) => format!(
            "{ADDRESSED_FACT_REF_PREFIX}{source}.{}{CAMP_ROLE_SEPARATOR}lead",
            camp_field(camp)
        ),
        Some(Reason::CampRole { camp, role }) => {
            let prefix = if role == "watch" {
                CAMP_WATCH_FACT_REF_PREFIX
            } else {
                ADDRESSED_FACT_REF_PREFIX
            };
            format!(
                "{prefix}{source}.{}{CAMP_ROLE_SEPARATOR}{role}",
                camp_field(camp)
            )
        }
        Some(Reason::Backfilled { clause }) => clause.clone(),
        Some(Reason::Subscription { .. } | Reason::Parent { .. }) | None => {
            format!("{ADDRESSED_FACT_REF_PREFIX}{source}.{field}")
        }
    }
}

/// Separates a camp-delivered clause ref from the role its recipient was
/// reached at, so the delivered line can say which camp it came from and why.
const CAMP_ROLE_SEPARATOR: &str = "@";

/// The camp a delivered wake came through and the role it reached its
/// recipient at, read back from the clause ref the index lane wrote.
fn camp_delivery<'a>(clause_ref: &'a str, row: &FactRow) -> Option<(String, &'a str)> {
    let rest = clause_ref
        .strip_prefix(ADDRESSED_FACT_REF_PREFIX)
        .or_else(|| clause_ref.strip_prefix(CAMP_WATCH_FACT_REF_PREFIX))?;
    let (path, role) = rest.rsplit_once(CAMP_ROLE_SEPARATOR)?;
    let field = path.rsplit_once('.')?.1;
    let fields: HashMap<String, Value> = serde_json::from_str(&row.fields_json).ok()?;
    let camp = fields.get(field)?.as_str()?;
    match cairn_uri::parse_uri(camp)? {
        cairn_uri::CairnResource::ProjectCamp { slug, .. } => Some((slug, role)),
        _ => None,
    }
}

/// The recipients whose own write a row is: the job that minted it, and the
/// home the source's declared actor field names. Both are stable recipients,
/// so a thread that rotated sessions between mint and indexing is not handed
/// its own writing on the new session.
async fn actor_recipients(
    orch: &Orchestrator,
    row: &FactRow,
    spec: &super::SourceSpec,
    fields: &BTreeMap<String, Value>,
) -> Vec<StableRecipient> {
    let mut actors = Vec::new();
    if let Some(job) = &row.actor_job_id {
        match cairn_attention::wakes::stable_recipient_for_job(&orch.db.local, job).await {
            Ok(recipient) => actors.push(recipient),
            Err(error) => log::warn!("could not resolve actor job of fact {}: {error}", row.id),
        }
    }
    let home = spec
        .actor_home
        .and_then(|field| fields.get(field))
        .and_then(Value::as_str);
    if let Some(home) = home {
        match crate::orchestrator::wakes::home_recipient_for_uri(&orch.db.local, home).await {
            Ok(Some(recipient)) if !actors.contains(&recipient) => actors.push(recipient),
            Ok(_) => {}
            Err(error) => log::warn!(
                "could not resolve actor home {home} of fact {}: {error}",
                row.id
            ),
        }
    }
    actors
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

/// The receipt key of a logged fact: its source and row. A row is one fact;
/// a transition-deduped source records a state that recurs as a new row, so
/// the row, not the identity, is what tells a recurrence from a re-index.
pub(crate) fn fact_log_key(row: &FactRow) -> String {
    format!("fact_log:{}:{}", row.source, row.id)
}

/// Write the receipt `row` owes `candidate`, fold the row into its pending
/// wake, and record both on the row.
async fn fold_wake(
    orch: &Orchestrator,
    wakes: &mut Vec<Value>,
    row: &FactRow,
    candidate: &Candidate,
    clause_ref: String,
    via: Option<&str>,
    window: WakeWindow,
) {
    let fact_key = fact_log_key(row);
    let matched = WakeMatch {
        recipient_key: candidate.recipient.key(),
        source: row.source.clone(),
        clause_ref,
        level: candidate.level.as_str().to_string(),
        project_id: row.project_id.clone(),
        row_id: row.id,
        at_ms: row.minted_at,
    };
    let new = NewReceipt {
        fact_key: &fact_key,
        candidate,
        project_id: row.project_id.as_deref(),
        fact_row_id: Some(row.id),
    };
    let (action, wake) =
        match cairn_attention::wakes::queue_fact_receipt(&orch.db.local, &new, &matched, window)
            .await
        {
            Ok((ReceiptWrite::Consumed(_), wake)) => ("already delivered".to_string(), wake),
            Ok((_, wake)) => ("coalesced".to_string(), wake),
            Err(error) => (format!("failed: {error}"), None),
        };
    if wakes.len() >= MAX_RECORDED_WAKES {
        return;
    }
    let job = recipient_job(orch, &candidate.recipient).await;
    let home = match &job {
        Some(job) => crate::jobs::queries::home_uri_for_job(&orch.db.local, job)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let mut record = serde_json::json!({
        "job": job.unwrap_or_else(|| candidate.recipient.key()),
        "recipient": candidate.recipient.key(),
        "uri": home,
        "action": action,
        "wake": wake,
        "level": candidate.level.as_str(),
        "reasons": candidate.reasons,
    });
    if let Some(via) = via {
        record["via"] = Value::String(via.to_string());
    }
    wakes.push(record);
}

/// The job a recipient reads as now: a thread's current session, or the job.
async fn recipient_job(orch: &Orchestrator, recipient: &StableRecipient) -> Option<String> {
    match recipient {
        StableRecipient::Job(job) => Some(job.clone()),
        StableRecipient::Thread(thread_id) => {
            let thread_id = thread_id.clone();
            orch.db
                .local
                .read(move |conn| {
                    let thread_id = thread_id.clone();
                    Box::pin(async move {
                        cairn_attention::wakes::thread_session_conn(conn, &thread_id).await
                    })
                })
                .await
                .ok()
                .flatten()
        }
    }
}

/// Deliver every coalesced wake due by `due_by_ms`. Returns how many it
/// claimed. Each wake is claimed before it is sent; a claim a restart caught
/// is delivered again, and its receipts keep that from delivering anything
/// twice.
pub(crate) async fn flush_due_wakes(orch: &Orchestrator, due_by_ms: i64) -> Result<usize, String> {
    flush_due_wakes_with(orch, due_by_ms, &|recipient, project_id| {
        wake_deferral(orch, recipient, project_id)
    })
    .await
}

/// The predicate the drain asks whether to wake a recipient yet.
pub(crate) type DeferralFn<'a> =
    dyn Fn(&StableRecipient, Option<&str>) -> Option<DeferUntil> + Send + Sync + 'a;

/// [`flush_due_wakes`] under a given deferral predicate.
pub(crate) async fn flush_due_wakes_with(
    orch: &Orchestrator,
    due_by_ms: i64,
    deferral: &DeferralFn<'_>,
) -> Result<usize, String> {
    let mut flushed = 0;
    // A wake requeued during this pass is not claimed again in it, so a
    // recipient that cannot take its wake yet never spins the pass.
    let mut seen = std::collections::HashSet::new();
    while let Some(wake) = crate::storage::claim_due_fact_wake(
        &orch.db.local,
        due_by_ms,
        chrono::Utc::now().timestamp_millis(),
    )
    .await
    .map_err(|error| error.to_string())?
    {
        if !seen.insert(wake.id) {
            crate::storage::requeue_fact_wake(
                &orch.db.local,
                wake.id,
                chrono::Utc::now()
                    .timestamp_millis()
                    .saturating_add(WAKE_RETRY_MS),
            )
            .await
            .map_err(|error| error.to_string())?;
            break;
        }
        flushed += 1;
        match deliver_wake(orch, &wake, deferral).await {
            WakeSettlement::Finished { action, job_id } => crate::storage::finish_fact_wake(
                &orch.db.local,
                wake.id,
                &action,
                job_id.as_deref(),
            )
            .await
            .map_err(|error| error.to_string())?,
            WakeSettlement::Requeued { at_ms } => crate::storage::requeue_fact_wake(
                &orch.db.local,
                wake.id,
                at_ms.max(due_by_ms.saturating_add(1)),
            )
            .await
            .map_err(|error| error.to_string())?,
        }
    }
    Ok(flushed)
}

/// How long a wake the recipient could not take waits before the next
/// attempt.
const WAKE_RETRY_MS: i64 = 30_000;

/// What delivering one claimed wake left it as.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WakeSettlement {
    Finished {
        action: String,
        job_id: Option<String>,
    },
    /// Owed again at `at_ms`: its recipient could not take it yet, or more
    /// receipts joined it while it was being delivered.
    Requeued { at_ms: i64 },
}

fn finished(action: &str, job_id: Option<String>) -> WakeSettlement {
    WakeSettlement::Finished {
        action: action.to_string(),
        job_id,
    }
}

/// The urgency a receipt level delivers at.
fn urgency_for(level: Wake) -> DeliveryUrgency {
    match level {
        Wake::Passive => DeliveryUrgency::Passive,
        Wake::Wake => DeliveryUrgency::Queue,
        Wake::Interrupt => DeliveryUrgency::Interrupt,
    }
}

/// Deliver one claimed wake from its receipts.
///
/// 1. A receipt already bound to a message (an attempt a crash or a failure
///    left unsettled) re-settles exactly that message first, alone.
/// 2. Each queued receipt's camp reasons are re-validated against the camp
///    as it stands now; a receipt left with no reason is withdrawn.
/// 3. The recipient's current session is resolved now, never at indexing.
/// 4. The existing gates refuse a closed, fenced, cancelled or resolved
///    recipient, whose receipts are dropped and never resurrected.
/// 5. A deferral (startup recovery, the boot ramp) holds a waking receipt
///    without touching its level, until it clears or its deadline passes; a
///    busy recipient sees it passively at its next boundary, which delivers it.
/// 6. One message carries the receipts, under an id derived from the
///    recipient and the sorted fact keys, settled with its push and receipts
///    in one transaction (`parent_wake::deliver_direct`).
async fn deliver_wake(
    orch: &Orchestrator,
    wake: &FactWake,
    deferral: &DeferralFn<'_>,
) -> WakeSettlement {
    let now = chrono::Utc::now().timestamp_millis();
    let retry = WakeSettlement::Requeued {
        at_ms: now.saturating_add(WAKE_RETRY_MS),
    };
    let db = &orch.db.local;
    let Some(recipient) = StableRecipient::parse(&wake.recipient_key) else {
        return finished("failed: unknown recipient", None);
    };
    let receipts = match cairn_attention::wakes::receipts_for_wake(db, wake.id).await {
        Ok(receipts) => receipts,
        Err(error) => {
            log::warn!("wake {}: could not read its receipts: {error}", wake.id);
            return retry;
        }
    };
    if receipts.is_empty() {
        return finished("delivered", wake.job_id.clone());
    }

    if let Some(message_id) = receipts
        .iter()
        .find(|receipt| receipt.state == receipt_state::DELIVERING)
        .and_then(|receipt| receipt.message_id.clone())
    {
        let bound: Vec<_> = receipts
            .iter()
            .filter(|receipt| receipt.message_id.as_deref() == Some(message_id.as_str()))
            .cloned()
            .collect();
        let level = bound
            .iter()
            .map(|receipt| receipt.level)
            .max()
            .unwrap_or(Wake::Passive);
        return send_wake(orch, wake, &recipient, &bound, level, &message_id, false).await;
    }

    let mut owed = Vec::new();
    for receipt in receipts {
        let reasons = {
            let (recipient, reasons) = (recipient.clone(), receipt.reasons.clone());
            db.read(move |conn| {
                let (recipient, reasons) = (recipient.clone(), reasons.clone());
                Box::pin(async move {
                    cairn_attention::wakes::revalidate_reasons_conn(conn, &recipient, reasons).await
                })
            })
            .await
        };
        let reasons = match reasons {
            Ok(reasons) => reasons,
            Err(error) => {
                log::warn!("wake {}: could not re-validate a receipt: {error}", wake.id);
                return retry;
            }
        };
        if reasons.is_empty() {
            let _ = cairn_attention::wakes::close_receipts(
                db,
                std::slice::from_ref(&receipt.id),
                receipt_state::WITHDRAWN,
                "every reason it was owed for is gone",
            )
            .await;
            continue;
        }
        let level = reasons
            .iter()
            .map(|reached| reached.level)
            .max()
            .unwrap_or(Wake::Passive);
        if level != receipt.level || reasons != receipt.reasons {
            let _ = cairn_attention::wakes::relevel_receipt(db, &receipt.id, level, &reasons).await;
        }
        owed.push(cairn_attention::wakes::Receipt {
            level,
            reasons,
            ..receipt
        });
    }
    if owed.is_empty() {
        return finished("withdrawn", None);
    }
    let ids: Vec<String> = owed.iter().map(|receipt| receipt.id.clone()).collect();
    let level = owed
        .iter()
        .map(|receipt| receipt.level)
        .max()
        .unwrap_or(Wake::Passive);

    let job_id = match deliverable_job(orch, &recipient, level.wakes_idle()).await {
        Ok(Ok(job_id)) => job_id,
        Ok(Err(why)) => {
            let _ =
                cairn_attention::wakes::close_receipts(db, &ids, receipt_state::DROPPED, why).await;
            return finished("dropped", None);
        }
        Err(error) => {
            let _ = cairn_attention::wakes::note_receipt_refusal(db, &ids, &error).await;
            return retry;
        }
    };
    let owning = crate::execution::routing::owning_db_for_job(&orch.db, &job_id)
        .await
        .unwrap_or_else(|_| db.clone());
    match crate::messages::delivery::delivery_refusal(&owning, &job_id).await {
        Ok(Some(refusal)) => {
            let _ = cairn_attention::wakes::close_receipts(
                db,
                &ids,
                receipt_state::DROPPED,
                refusal.describe(),
            )
            .await;
            return finished("dropped", Some(job_id));
        }
        Ok(None) => {}
        Err(error) => {
            let _ = cairn_attention::wakes::note_receipt_refusal(db, &ids, &error).await;
            return retry;
        }
    }
    if job_issue_is_resolved(&owning, &job_id).await {
        let _ = cairn_attention::wakes::close_receipts(
            db,
            &ids,
            receipt_state::DROPPED,
            "its issue is resolved",
        )
        .await;
        return finished("dropped", Some(job_id));
    }

    let mut passive_now = false;
    if level.wakes_idle() {
        let decision = cairn_attention::wakes::admit_wake(
            now,
            [deferral(&recipient, wake.project_id.as_deref())],
        );
        if let Admission::Defer(defer) = decision {
            let busy = crate::messages::delivery::head_turn_active(&owning, &job_id)
                .await
                .unwrap_or(false);
            if !busy {
                let _ = cairn_attention::wakes::note_receipt_refusal(
                    db,
                    &ids,
                    &format!("deferred: {}", defer.reason),
                )
                .await;
                return WakeSettlement::Requeued {
                    at_ms: cairn_attention::wakes::deferral_recheck_at(now, &defer),
                };
            }
            passive_now = true;
        }
    }
    let mut keys: Vec<&str> = owed
        .iter()
        .map(|receipt| receipt.fact_key.as_str())
        .collect();
    keys.sort_unstable();
    let message_id = cairn_attention::wakes::delivery_message_id(&recipient, &keys);
    if let Err(error) = cairn_attention::wakes::bind_receipts(db, &ids, &message_id).await {
        log::warn!("wake {}: could not bind its receipts: {error}", wake.id);
        return retry;
    }
    let bound: Vec<_> = owed
        .into_iter()
        .map(|receipt| cairn_attention::wakes::Receipt {
            state: receipt_state::DELIVERING.to_string(),
            message_id: Some(message_id.clone()),
            ..receipt
        })
        .collect();
    send_wake(
        orch,
        wake,
        &recipient,
        &bound,
        level,
        &message_id,
        passive_now,
    )
    .await
}

/// Settle one message carrying `bound` receipts, then requeue the wake if
/// receipts joined it meanwhile, so they go in a later message alone.
async fn send_wake(
    orch: &Orchestrator,
    wake: &FactWake,
    recipient: &StableRecipient,
    bound: &[cairn_attention::wakes::Receipt],
    level: Wake,
    message_id: &str,
    passive_now: bool,
) -> WakeSettlement {
    let db = &orch.db.local;
    let now = chrono::Utc::now().timestamp_millis();
    let ids: Vec<String> = bound.iter().map(|receipt| receipt.id.clone()).collect();
    let job_id = match deliverable_job(orch, recipient, level.wakes_idle()).await {
        Ok(Ok(job_id)) => job_id,
        Ok(Err(why)) => {
            let _ =
                cairn_attention::wakes::close_receipts(db, &ids, receipt_state::DROPPED, why).await;
            return finished("dropped", None);
        }
        Err(_) => {
            return WakeSettlement::Requeued {
                at_ms: now.saturating_add(WAKE_RETRY_MS),
            }
        }
    };
    let (message, _) = wake_message(orch, wake).await;
    let urgency = if passive_now {
        DeliveryUrgency::Passive
    } else {
        urgency_for(level)
    };
    let mut request = DeliveryRequest::new(&job_id, &message, urgency);
    request.message_id = Some(message_id);
    request.receipts = &ids;
    match crate::orchestrator::parent_wake::deliver_direct(orch, request).await {
        DeliveryOutcome::Persisted { job_id, .. } | DeliveryOutcome::Launched { job_id } => {
            let left = cairn_attention::wakes::receipts_for_wake(db, wake.id)
                .await
                .map(|receipts| !receipts.is_empty())
                .unwrap_or(false);
            if left {
                return WakeSettlement::Requeued { at_ms: now };
            }
            // A passive line rides along with the recipient's next run.
            let action = if urgency == DeliveryUrgency::Passive {
                "suppressed"
            } else {
                "delivered"
            };
            finished(action, Some(job_id))
        }
        DeliveryOutcome::Refused(why) => {
            let _ = cairn_attention::wakes::close_receipts(db, &ids, receipt_state::DROPPED, &why)
                .await;
            finished("dropped", Some(job_id))
        }
        DeliveryOutcome::Failed(why) => {
            log::warn!("wake {} to {}: {why}", wake.id, recipient.key());
            WakeSettlement::Requeued {
                at_ms: now.saturating_add(WAKE_RETRY_MS),
            }
        }
    }
}

/// The job a delivery to `recipient` lands on now, in the recipient's own
/// database: a thread's newest session, established when it has none and the
/// delivery would wake it, or the job itself.
async fn deliverable_job(
    orch: &Orchestrator,
    recipient: &StableRecipient,
    establish: bool,
) -> Result<Result<String, &'static str>, String> {
    let db = crate::execution::routing::routing_db_for_id(&orch.db, recipient.id())
        .await
        .map_err(|error| error.to_string())?;
    let recipient = recipient.clone();
    db.write(move |conn| {
        let recipient = recipient.clone();
        Box::pin(async move {
            cairn_attention::wakes::deliverable_job_conn(
                std::sync::Arc::new(crate::attention_host::CoreRecipients),
                conn,
                &recipient,
                establish,
            )
            .await
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Whether the job belongs to a merged or closed issue. Fails closed.
async fn job_issue_is_resolved(db: &crate::storage::LocalDb, job_id: &str) -> bool {
    use cairn_attention::runtime::RecipientHost;
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            crate::attention_host::CoreRecipients
                .job_issue_is_resolved(conn, &job_id)
                .await
        })
    })
    .await
    .unwrap_or(true)
}

/// Whether to wake `recipient` yet: the one deferral seam the drain asks.
///
/// Every source is bounded, so a predicate that never clears still admits at
/// its deadline. While the runner's startup recovery has not finished, a
/// wake waits for it, at most until the end of the boot ramp horizon. While
/// the boot ramp's spawn window is full, a wake that would launch another
/// process waits for a slot, bounded the same way. Only an authenticated
/// direct operator message is not deferred, and it never comes through here:
/// it is a direct delivery, not a fact.
pub(crate) fn wake_deferral(
    orch: &Orchestrator,
    _recipient: &StableRecipient,
    _project_id: Option<&str>,
) -> Option<DeferUntil> {
    let deadline_ms = orch
        .boot_at
        .saturating_mul(1000)
        .saturating_add(crate::execution::jobs::BOOT_RAMP_HORIZON.as_millis() as i64);
    if !orch.startup_readiness.is_ready() {
        return Some(DeferUntil {
            deadline_ms,
            reason: "startup recovery has not finished".to_string(),
        });
    }
    if crate::execution::jobs::boot_ramp_active(orch) && orch.private_lanes.spawn_window().is_full()
    {
        return Some(DeferUntil {
            deadline_ms,
            reason: "the boot ramp's spawn window is full".to_string(),
        });
    }
    None
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
            if let Some((camp, role)) = camp_delivery(&wake.clause_ref, &row) {
                return (
                    format!("From camp {camp} (your role: {role}): {summary} — {uri}"),
                    uri,
                );
            }
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
        Some(key) => (cairn_uri::build_project_events_uri(key), key.to_string()),
        None => (
            cairn_uri::build_workspace_events_uri(),
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
        Some(spec) => actor_recipients(orch, row, spec, &stored_fact(row)?.fields).await,
        None => Vec::new(),
    };
    let candidate = Fact {
        source: &fact.source,
        fields: &fact.fields,
        presence: super::log::presence_from_str(&row.presence),
    };
    // The consumers whose thread is an actor of the row.
    let mut consumer_threads = std::collections::HashSet::new();
    for consumer in &own {
        if let Ok(recipient) =
            cairn_attention::wakes::stable_recipient_for_job(&orch.db.local, &consumer.job_id).await
        {
            if actors.contains(&recipient) {
                consumer_threads.insert(consumer.job_id.clone());
            }
        }
    }
    Ok(own
        .into_iter()
        .map(|consumer| {
            let mut reasons = super::explain_clause(
                &consumer.clause,
                &candidate,
                Some(&consumer.scope(row.project_id.as_deref())),
            );
            let own = row.actor_job_id.as_deref() == Some(consumer.job_id.as_str())
                || actors.iter().any(|actor| match actor {
                    StableRecipient::Job(job) => *job == consumer.job_id,
                    StableRecipient::Thread(_) => consumer_threads.contains(&consumer.job_id),
                });
            if own {
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
    let Some(home) =
        crate::orchestrator::wakes::home_recipient_for_uri(&orch.db.local, home_uri).await?
    else {
        return Ok(None);
    };
    let host: std::sync::Arc<dyn cairn_attention::runtime::RecipientHost> =
        std::sync::Arc::new(crate::attention_host::CoreRecipients);
    for field in spec.addressees {
        for uri in addressee_uris(&stored.fields, field) {
            let mut audience = FactAudience::new(&row.source, Wake::Wake, Wake::Wake);
            let db = match crate::orchestrator::wakes::addressed(&orch.db, &uri).await? {
                Addressed::Home(recipient) => {
                    if recipient == home {
                        return Ok(Some(field));
                    }
                    continue;
                }
                Addressed::Issue { db, uri } => {
                    audience.source = Some(SourceRef::issue(&uri));
                    db
                }
                Addressed::Camp { db, camp_id } => {
                    audience.camps.push(camp_id);
                    db
                }
                Addressed::Nothing => continue,
            };
            let found =
                cairn_attention::wakes::resolve_recipients(host.clone(), &db, &audience).await?;
            if found.iter().any(|candidate| candidate.recipient == home) {
                return Ok(Some(field));
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
    let routes = match orch.fact_log.index.routes(
        &orch.config_dir,
        path.as_deref(),
        orch.services.clock.now(),
    ) {
        Ok(routes) => routes,
        Err(error) => return (route_state::FAILED, Some(format!("routes: {error}"))),
    };
    // The submissions live outside the timed future: a timeout cancels the
    // route that hung, not the deliveries the routes before it prepared.
    let mut submissions = Vec::new();
    let event = event_uri(orch, &row).await;
    let result = tokio::time::timeout(
        timeout,
        super::dispatcher::dispatch_row(
            orch,
            &hydrated.fact,
            hydrated.presence,
            RouteContext {
                project_id: row.project_id.as_deref(),
                project_path: path.as_deref(),
                event: Some(super::LoggedEvent {
                    id: row.id,
                    uri: &event,
                }),
            },
            &routes,
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
    // A settled receipt is history once its fact is: the default retention.
    let receipt_cutoff = now.saturating_sub(policy.default_retention_ms);
    for db in orch.db.all_dbs().await {
        while crate::storage::prune_delivery_receipts(&db, receipt_cutoff, PRUNE_BATCH)
            .await
            .map_err(|error| error.to_string())?
            == PRUNE_BATCH as u64
        {
            tokio::task::yield_now().await;
        }
    }
    // The records `ingress` rows point at keep the rows' own retention.
    let ingress_cutoff = now.saturating_sub(
        policy
            .retention_ms
            .get("ingress")
            .copied()
            .unwrap_or(policy.default_retention_ms),
    );
    while crate::ingress::prune_events(&orch.db.local, ingress_cutoff, PRUNE_BATCH).await?
        == PRUNE_BATCH as u64
    {
        tokio::task::yield_now().await;
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
    // Bounded: a wake whose recipient never takes it is requeued forever, and
    // a drain that treats every wake as due would otherwise never settle.
    for _ in 0..64 {
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
        orchestrator_with(temp, routes, TestServicesBuilder::new()).await
    }

    /// [`orchestrator`] over the given services, for a test that needs a git
    /// or HTTP mock of its own.
    pub(crate) async fn orchestrator_with(
        temp: &Path,
        routes: &[(&str, &str)],
        services: TestServicesBuilder,
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
            Arc::new(services.build()),
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
#[path = "worker_ambient_tests.rs"]
mod ambient_tests;

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
             INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tq2','q','api','active',1,1);
             INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('other-writer','tq2','q','running','thread',1,1);
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
                    answer_bound_ms: None,
                    thread_id: None,
                    cwd: "/tmp".to_string(),
                    run_id: None,
                    tool: "read".to_string(),
                    payload: serde_json::json!({}),
                    tool_use_id: None,
                    origin: None,
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

        async fn messages_to(db: &crate::storage::LocalDb, run: &str) -> Vec<String> {
            db.query_all(
                "SELECT id FROM messages WHERE recipient_run_id = ?1 ORDER BY rowid",
                (run.to_string(),),
                |row| crate::storage::RowExt::text(row, 0),
            )
            .await
            .unwrap()
        }

        /// B2: a crash after a wake's receipts were bound to their message
        /// re-settles exactly that message, and a receipt that arrived before
        /// the retry goes in a later message alone. No receipt is carried twice.
        #[tokio::test]
        async fn a_bound_wake_resettles_its_message_and_a_late_receipt_goes_alone() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            standing_triggers(&db, "watcher", r#"[{"fact":"github_comment"}]"#).await;
            let policy = EventLogPolicy::default();
            row_at(&db, 1, T0, None).await;
            row_at(&db, 2, T0 + SECOND, None).await;
            index_pass(&orch, &policy).await.unwrap();

            let wake = crate::storage::claim_due_fact_wake(&db, i64::MAX, T0 + 60 * SECOND)
                .await
                .unwrap()
                .unwrap();
            let bound = cairn_attention::wakes::receipts_for_wake(&db, wake.id)
                .await
                .unwrap();
            assert_eq!(bound.len(), 2);
            let keys: Vec<&str> = bound.iter().map(|r| r.fact_key.as_str()).collect();
            let message_id = cairn_attention::wakes::delivery_message_id(
                &StableRecipient::Thread("t".into()),
                &keys,
            );
            let ids: Vec<String> = bound.iter().map(|r| r.id.clone()).collect();
            cairn_attention::wakes::bind_receipts(&db, &ids, &message_id)
                .await
                .unwrap();
            // The runner stops here, before the settlement transaction.
            crate::storage::recover_fact_log(&db, T0 + 120 * SECOND, 0)
                .await
                .unwrap();
            row_at(&db, 3, T0 + 2 * SECOND, None).await;
            drain_fact_log(&orch).await;

            let messages = messages_to(&db, "watcher-run").await;
            assert_eq!(messages.len(), 2, "{messages:?}");
            assert_eq!(
                messages[0], message_id,
                "the bound set re-settles its own message"
            );
            let carried = db
                .query_all(
                    "SELECT message_id FROM delivery_receipts WHERE state = 'delivered' ORDER BY created_at, id",
                    (),
                    |row| crate::storage::RowExt::text(row, 0),
                )
                .await
                .unwrap();
            assert_eq!(carried.len(), 3, "every receipt is delivered once");
            assert_eq!(
                carried.iter().filter(|id| **id == message_id).count(),
                2,
                "the late receipt went in a message of its own"
            );
        }

        /// A thread that rotates sessions between indexing and delivery is told
        /// on its new session.
        #[tokio::test]
        async fn a_rotated_session_receives_the_wake_owed_to_its_thread() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            standing_triggers(&db, "watcher", r#"[{"fact":"github_comment"}]"#).await;
            row_at(&db, 1, T0, None).await;
            index_pass(&orch, &EventLogPolicy::default()).await.unwrap();
            db.execute_batch(
                "INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('watcher-new','t','p','running','thread',5,5);
                 INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('watcher-new-run','watcher-new','running',5,5);",
            )
            .await
            .unwrap();
            drain_fact_log(&orch).await;
            assert_eq!(events_received(&db, "watcher-new-run").await.len(), 1);
            assert!(events_received(&db, "watcher-run").await.is_empty());
        }

        /// A deferral holds a wake at its stored level until its deadline, and
        /// a predicate that never clears still admits once the deadline passes.
        #[tokio::test]
        async fn a_deferred_wake_keeps_its_level_and_admits_at_its_deadline() {
            let temp = tempfile::tempdir().unwrap();
            let (orch, db) = orchestrator(temp.path(), &[]).await;
            standing_triggers(&db, "watcher", r#"[{"fact":"github_comment"}]"#).await;
            row_at(&db, 1, T0, None).await;
            index_pass(&orch, &EventLogPolicy::default()).await.unwrap();
            let now = chrono::Utc::now().timestamp_millis();
            let later = |_: &StableRecipient, _: Option<&str>| {
                Some(DeferUntil {
                    deadline_ms: now + 3_600_000,
                    reason: "never clears".into(),
                })
            };
            flush_due_wakes_with(&orch, i64::MAX, &later).await.unwrap();
            assert!(events_received(&db, "watcher-run").await.is_empty());
            let held = db
                .query_one(
                    "SELECT state, level, last_refusal FROM delivery_receipts",
                    (),
                    |row| {
                        Ok((
                            crate::storage::RowExt::text(row, 0)?,
                            crate::storage::RowExt::text(row, 1)?,
                            crate::storage::RowExt::opt_text(row, 2)?,
                        ))
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                held,
                (
                    "queued".to_string(),
                    "wake".to_string(),
                    Some("deferred: never clears".to_string())
                ),
                "a deferral never rewrites the stored level"
            );

            let passed = |_: &StableRecipient, _: Option<&str>| {
                Some(DeferUntil {
                    deadline_ms: now - 1,
                    reason: "never clears".into(),
                })
            };
            crate::storage::requeue_fact_wake(&db, 1, 0).await.unwrap();
            flush_due_wakes_with(&orch, i64::MAX, &passed)
                .await
                .unwrap();
            assert_eq!(events_received(&db, "watcher-run").await.len(), 1);
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
        async fn a_wake_caught_by_a_restart_is_delivered_once_with_its_count() {
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
            assert_eq!(claimed.recipient_key, "thread:t");

            let recovery = crate::storage::recover_fact_log(&db, T0 + 120 * SECOND, 0)
                .await
                .unwrap();
            assert_eq!(recovery.requeued_wakes, 1);
            assert_eq!(flush_due_wakes(&orch, T0 + 120 * SECOND).await.unwrap(), 2);
            assert_eq!(flush_due_wakes(&orch, i64::MAX).await.unwrap(), 0);

            let (first, last) = (ids[0], ids[4]);
            let expected = vec![format!(
                "[Event] 5 github_comment rows in cairn (ids {first}\u{2013}{last}): cairn://p/cairn/events?source=github_comment&from={first}&to={last}"
            )];
            assert_eq!(
                events_received(&db, "watcher2-run").await,
                expected,
                "the pending wake survives the restart with its count and range"
            );
            assert_eq!(
                events_received(&db, "watcher-run").await,
                expected,
                "a wake caught mid-delivery is delivered again, once: its receipts were still owed"
            );
            assert_eq!(
                flush_due_wakes(&orch, i64::MAX).await.unwrap(),
                0,
                "and nothing is owed after it"
            );
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
