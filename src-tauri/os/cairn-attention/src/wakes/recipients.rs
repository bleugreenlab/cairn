//! The one recipient resolver.
//!
//! Every way a fact can owe a recipient a delivery is derived here, in one
//! function, so the fact index, issue pushes and subscription broadcasts can
//! never disagree about who hears a fact or how loudly:
//!
//! 1. the parent/coordinator edge of an issue-sourced fact
//!    ([`super::child::gate_owner_for_child_conn`]);
//! 2. persisted subscriptions on the fact's source, filtered by each row's
//!    fact kinds;
//! 3. camps: every thread member of a camp the fact addresses, passively for
//!    base camp and at the watcher level for a camp;
//! 4. standing triggers and direct addressees, which the host resolves (they
//!    are application concepts) and offers here.
//!
//! Candidates merge by [`StableRecipient`]: a thread, never one of its
//! sessions, or a non-session job. Each keeps the **maximum** level any reason
//! reaches it at and **every** reason, so a passive base camp line never
//! lowers a wake and a mute removes only its own subscription's reason. The session that
//! receives a delivery is resolved at delivery, never here.
//!
//! Exclusions (the fact's own actors, the producing node, the subject issue's
//! own jobs) and jurisdiction are applied here too, on every call. An archived
//! camp contributes nothing, which every retry re-checks because every retry
//! resolves again.

use std::collections::{BTreeMap, BTreeSet};

use cairn_db::storage::{DbResult, LocalDb, RowExt, TrackedConnection};
use cairn_db::turso::params;
use cairn_domain::{Camp, CampSubject};
use serde::{Deserialize, Serialize};

use crate::push::Wake;

use super::child::{gate_owner_for_child_conn, GateOwner};
use super::matching::best_matching_subscription;
use super::types::*;

/// Who a fact is owed to, independent of which session is current.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StableRecipient {
    /// A thread (`threads.id`): delivered to its newest session at delivery.
    Thread(String),
    /// A job that is not a thread session: a coordinator, a sub-agent task.
    Job(String),
}

impl StableRecipient {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Thread(_) => "thread",
            Self::Job(_) => "job",
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Thread(id) | Self::Job(id) => id,
        }
    }

    /// `thread:<id>` or `job:<id>`, the form a coalesced wake stores.
    pub fn key(&self) -> String {
        format!("{}:{}", self.kind(), self.id())
    }

    pub fn from_parts(kind: &str, id: &str) -> Option<Self> {
        match kind {
            "thread" => Some(Self::Thread(id.to_string())),
            "job" => Some(Self::Job(id.to_string())),
            _ => None,
        }
    }

    pub fn parse(key: &str) -> Option<Self> {
        let (kind, id) = key.split_once(':')?;
        Self::from_parts(kind, id)
    }
}

/// Why a recipient is owed a fact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Reason {
    /// The node or thread coordinating the fact's issue.
    Parent { issue: String },
    /// A persisted subscription on the fact's source. A muted one still
    /// reaches its recipient, passively.
    Subscription {
        id: String,
        #[serde(default)]
        muted: bool,
    },
    /// A member of a camp the fact addresses. The level is the camp's kind:
    /// passive for base camp, the watcher level for a camp.
    Camp { camp: String },
    /// A thread standing trigger; `consumer` is its clause ref.
    Trigger { consumer: String },
    /// The fact names the recipient's home in one of its addressee fields.
    Addressed { field: String },
    /// A wake that was pending when receipts were introduced.
    Backfilled { clause: String },
}

impl Reason {
    /// Whether jurisdiction (the recipient's project against the fact's) may
    /// strike this reason. A parent edge, an explicit subscription and a
    /// trigger each name their subject deliberately, and a trigger applies its
    /// own `project` test before it is offered.
    fn is_jurisdictional(&self) -> bool {
        matches!(self, Self::Camp { .. } | Self::Addressed { .. })
    }
}

/// One reason and the level it reaches its recipient at.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reached {
    #[serde(flatten)]
    pub reason: Reason,
    pub level: Wake,
}

/// One recipient a fact is owed to, at the maximum level of its reasons.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub recipient: StableRecipient,
    pub level: Wake,
    pub reasons: Vec<Reached>,
}

impl Candidate {
    fn recompute(&mut self) {
        self.reasons.sort_by(|a, b| a.reason.cmp(&b.reason));
        self.reasons.dedup_by(|a, b| {
            if a.reason == b.reason {
                b.level = b.level.max(a.level);
                true
            } else {
                false
            }
        });
        self.level = self
            .reasons
            .iter()
            .map(|reached| reached.level)
            .max()
            .unwrap_or(Wake::Passive);
    }

    /// Whether every reason is a muted subscription: the recipient said it
    /// only wants this source passively, and nothing else asked for more.
    pub fn only_muted(&self) -> bool {
        self.reasons
            .iter()
            .all(|reached| matches!(reached.reason, Reason::Subscription { muted: true, .. }))
    }

    /// The subscription rows this candidate was reached through.
    pub fn subscription_ids(&self) -> impl Iterator<Item = &str> {
        self.reasons
            .iter()
            .filter_map(|reached| match &reached.reason {
                Reason::Subscription { id, .. } => Some(id.as_str()),
                _ => None,
            })
    }
}

/// Candidates keyed by stable recipient: the one merge rule.
#[derive(Clone, Debug, Default)]
pub struct Audience(BTreeMap<StableRecipient, Candidate>);

impl Audience {
    pub fn offer(&mut self, recipient: StableRecipient, reason: Reason, level: Wake) {
        let candidate = self
            .0
            .entry(recipient.clone())
            .or_insert_with(|| Candidate {
                recipient,
                level,
                reasons: Vec::new(),
            });
        candidate.reasons.push(Reached { reason, level });
        candidate.recompute();
    }

    pub fn merge(&mut self, other: Audience) {
        for candidate in other.0.into_values() {
            for reached in candidate.reasons {
                self.offer(candidate.recipient.clone(), reached.reason, reached.level);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_candidates(self) -> Vec<Candidate> {
        self.0.into_values().collect()
    }

    fn retain(&mut self, mut keep: impl FnMut(&mut Candidate) -> bool) {
        self.0.retain(|_, candidate| keep(candidate));
    }
}

/// A persisted subscription source the fact is on.
#[derive(Clone, Debug)]
pub struct SourceRef {
    pub kind: String,
    pub reference: Option<String>,
}

impl SourceRef {
    pub fn issue(uri: &str) -> Self {
        Self {
            kind: SOURCE_KIND_ISSUE.to_string(),
            reference: Some(uri.to_string()),
        }
    }
}

/// Who a fact must not reach.
#[derive(Clone, Debug, Default)]
pub struct Exclusions {
    /// Jobs whose own write the fact is, and the producing node: each excludes
    /// its stable recipient, so a thread is not told its own writing on a
    /// session it has since rotated to.
    pub jobs: Vec<String>,
    /// Stable recipients excluded outright (an actor home already resolved).
    pub recipients: Vec<StableRecipient>,
    /// Exclude jobs that sit on the fact's own issue: a node is not told about
    /// its own issue's facts.
    pub subject_issue_jobs: bool,
    /// Apply jurisdiction: a jurisdictional reason reaches a recipient only
    /// when the fact is project-less or in the recipient's own project.
    pub jurisdiction: bool,
}

/// Everything the resolver needs to know about one fact.
#[derive(Clone, Debug)]
pub struct FactAudience {
    /// The fact's project, for jurisdiction.
    pub project_id: Option<String>,
    /// The subscription source the fact is on. An `issue` source also brings
    /// the issue's parent edge.
    pub source: Option<SourceRef>,
    pub fact_kind: String,
    /// Camps (by id, in the database being resolved against) the fact
    /// addresses directly.
    pub camps: Vec<String>,
    /// The level of an explicit subscription and of a camp member. A base
    /// camp member is always passive.
    pub watcher_level: Wake,
    /// The level of the parent/coordinator edge.
    pub parent_level: Wake,
    /// Triggers and direct addressees, resolved by the host.
    pub offered: Vec<(StableRecipient, Reason, Wake)>,
    pub exclude: Exclusions,
}

impl FactAudience {
    /// An audience with no source, camps or offers yet, at `watcher_level`
    /// for explicit watchers and `parent_level` for the coordinator.
    pub fn new(fact_kind: &str, watcher_level: Wake, parent_level: Wake) -> Self {
        Self {
            project_id: None,
            source: None,
            fact_kind: fact_kind.to_string(),
            camps: Vec::new(),
            watcher_level,
            parent_level,
            offered: Vec::new(),
            exclude: Exclusions::default(),
        }
    }
}

/// Resolve every recipient `audience` owes, against `db`.
pub async fn resolve_recipients(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    db: &LocalDb,
    audience: &FactAudience,
) -> Result<Vec<Candidate>, String> {
    let audience = audience.clone();
    db.read(move |conn| {
        let host = host.clone();
        let audience = audience.clone();
        Box::pin(async move {
            resolve_recipients_conn(host, conn, &audience)
                .await
                .map(Audience::into_candidates)
        })
    })
    .await
    .map_err(|error| format!("Failed to resolve a fact's recipients: {error}"))
}

/// [`resolve_recipients`] inside the caller's transaction.
pub async fn resolve_recipients_conn(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    conn: &TrackedConnection,
    audience: &FactAudience,
) -> DbResult<Audience> {
    let mut found = Audience::default();
    let mut subject_issue_id = None;
    if let Some(source) = &audience.source {
        let issue = match (source.kind.as_str(), source.reference.as_deref()) {
            (SOURCE_KIND_ISSUE, Some(uri)) => cairn_db::relations::resolve_issue_uri(conn, uri)
                .await?
                .map(|issue| (cairn_uri::canonicalize_uri_identity(uri), issue.issue_id)),
            _ => None,
        };
        let subscribed = subscriptions_conn(conn, source, audience).await?;
        let subscribed_recipients: BTreeSet<StableRecipient> = subscribed.keys().cloned().collect();
        for (recipient, offer) in subscribed {
            if let Some((reason, level)) = offer {
                found.offer(recipient, reason, level);
            }
        }
        if let Some((issue_uri, issue_id)) = issue {
            // A coordinator's own row that speaks to this fact governs it: a
            // mute or an unsubscribe cannot be widened back by the derived
            // parent watch, and a narrow row leaves the facts it does not name
            // to the watch. The derived watch carries the default child fact vocabulary, so a
            // fact outside it (an external message reply) still needs an
            // explicit subscription.
            let parent_watches = super::matching::subscription_accepts_fact(
                &derived_child_subscription("", &issue_uri),
                &audience.fact_kind,
            );
            if let (true, Some(parent)) = (
                parent_watches,
                parent_recipient_conn(host.clone(), conn, &issue_id).await?,
            ) {
                if !subscribed_recipients.contains(&parent) {
                    found.offer(
                        parent,
                        Reason::Parent { issue: issue_uri },
                        audience.parent_level,
                    );
                }
            }
            subject_issue_id = Some(issue_id);
        }
    }
    for camp_id in &audience.camps {
        camp_census_conn(conn, camp_id, audience.watcher_level, &mut found).await?;
    }
    for (recipient, reason, level) in &audience.offered {
        found.offer(recipient.clone(), reason.clone(), *level);
    }

    let mut excluded: BTreeSet<StableRecipient> =
        audience.exclude.recipients.iter().cloned().collect();
    for job in &audience.exclude.jobs {
        excluded.insert(stable_recipient_for_job_conn(conn, job).await?);
    }
    let mut same_issue = BTreeSet::new();
    let mut strangers = BTreeMap::new();
    for recipient in found.0.keys() {
        if let (true, Some(issue_id), StableRecipient::Job(job)) = (
            audience.exclude.subject_issue_jobs,
            subject_issue_id.as_deref(),
            recipient,
        ) {
            if job_issue_conn(conn, job).await?.as_deref() == Some(issue_id) {
                same_issue.insert(recipient.clone());
            }
        }
        if let (true, Some(project_id)) = (
            audience.exclude.jurisdiction,
            audience.project_id.as_deref(),
        ) {
            let home = recipient_project_conn(conn, recipient).await?;
            strangers.insert(recipient.clone(), home.as_deref() != Some(project_id));
        }
    }
    found.retain(|candidate| {
        if excluded.contains(&candidate.recipient) || same_issue.contains(&candidate.recipient) {
            return false;
        }
        if strangers
            .get(&candidate.recipient)
            .copied()
            .unwrap_or(false)
        {
            candidate
                .reasons
                .retain(|reached| !reached.reason.is_jurisdictional());
            candidate.recompute();
        }
        !candidate.reasons.is_empty()
    });
    Ok(found)
}

/// The persisted subscriptions on `source` that admit the fact's kind, grouped
/// by stable recipient: each recipient's best matching row decides its offer
/// (`None` when that row is unsubscribed). A recipient whose matching row
/// refuses still appears, because a row that speaks to this fact is what
/// keeps the derived parent watch from re-adding it; a row that does not
/// admit the fact kind says nothing about this fact, so the parent watch
/// still applies to it.
async fn subscriptions_conn(
    conn: &TrackedConnection,
    source: &SourceRef,
    audience: &FactAudience,
) -> DbResult<BTreeMap<StableRecipient, Option<(Reason, Wake)>>> {
    let standing_watch = i64::from(unscoped_row_is_a_standing_watch(&source.kind));
    let reference = source
        .reference
        .as_deref()
        .map(cairn_uri::canonicalize_uri_identity);
    let mut rows = conn
        .query(
            "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                    mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                    one_shot, match_phrase
             FROM wake_subscriptions
             WHERE source_kind = ?1 AND job_id IS NOT NULL
               AND (COALESCE(source_ref, '') = COALESCE(?2, '')
                    OR (?3 = 1 AND source_ref IS NULL))
             ORDER BY job_id ASC, created_at ASC, id ASC",
            params![source.kind.as_str(), reference.as_deref(), standing_watch],
        )
        .await?;
    let mut by_job: BTreeMap<String, Vec<WakeSubscription>> = BTreeMap::new();
    while let Some(row) = rows.next().await? {
        let subscription = subscription_from_row(&row)?;
        by_job
            .entry(subscription.job_id.clone())
            .or_default()
            .push(subscription);
    }
    drop(rows);
    let mut by_recipient: BTreeMap<StableRecipient, Vec<WakeSubscription>> = BTreeMap::new();
    for (job, subscriptions) in by_job {
        by_recipient
            .entry(stable_recipient_for_job_conn(conn, &job).await?)
            .or_default()
            .extend(subscriptions);
    }
    Ok(by_recipient
        .into_iter()
        .filter_map(|(recipient, subscriptions)| {
            let subscription = best_matching_subscription(
                subscriptions,
                &source.kind,
                reference.as_deref(),
                &audience.fact_kind,
            )?;
            let offer = match subscription.state {
                WakeSubscriptionState::Active => Some((
                    Reason::Subscription {
                        id: subscription.id,
                        muted: false,
                    },
                    audience.watcher_level,
                )),
                WakeSubscriptionState::Muted => Some((
                    Reason::Subscription {
                        id: subscription.id,
                        muted: true,
                    },
                    Wake::Passive,
                )),
                WakeSubscriptionState::Unsubscribed => None,
            };
            Some((recipient, offer))
        })
        .collect())
}

/// The stable recipient of an issue's gate owner, resolved without side
/// effects: a thread owner is the thread, never a session minted here.
async fn parent_recipient_conn(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    conn: &TrackedConnection,
    issue_id: &str,
) -> DbResult<Option<StableRecipient>> {
    Ok(
        match gate_owner_for_child_conn(host, conn, issue_id).await? {
            GateOwner::Thread(thread_id) => Some(StableRecipient::Thread(thread_id)),
            GateOwner::Job(job_id) => Some(stable_recipient_for_job_conn(conn, &job_id).await?),
            GateOwner::Operator => None,
        },
    )
}

/// Offer every thread member of an active camp. An archived camp offers
/// nothing, and the operator, who is never a delivery recipient, is skipped:
/// a camp the operator is in shows an unread count instead.
async fn camp_census_conn(
    conn: &TrackedConnection,
    camp_id: &str,
    watcher_level: Wake,
    found: &mut Audience,
) -> DbResult<()> {
    let Some(camp) = cairn_db::storage::camps::camp_by_id_conn(conn, camp_id).await? else {
        return Ok(());
    };
    if camp.is_archived() {
        return Ok(());
    }
    for member in cairn_db::storage::camps::camp_members_conn(conn, &camp).await? {
        let CampSubject::Thread { thread_id } = &member.subject else {
            continue;
        };
        let (reason, level) = camp_reason(&camp, watcher_level);
        found.offer(StableRecipient::Thread(thread_id.clone()), reason, level);
    }
    Ok(())
}

/// A camp member's reason and level: base camp is always passive, and a camp
/// reaches its members at the watcher level.
fn camp_reason(camp: &Camp, watcher_level: Wake) -> (Reason, Wake) {
    let level = if camp.is_base() {
        Wake::Passive
    } else {
        watcher_level
    };
    (
        Reason::Camp {
            camp: camp.id.clone(),
        },
        level,
    )
}

/// Re-check a queued receipt's camp reasons against the camp as it stands at
/// delivery: an archived camp, or a member that has left it (or closed),
/// withdraws the reason, and the level is never raised above what was stored.
/// Every other reason stands as stored.
pub async fn revalidate_reasons_conn(
    conn: &TrackedConnection,
    recipient: &StableRecipient,
    reasons: Vec<Reached>,
) -> DbResult<Vec<Reached>> {
    let mut kept = Vec::with_capacity(reasons.len());
    for reached in reasons {
        let camp_id = match &reached.reason {
            Reason::Camp { camp } => camp.clone(),
            _ => {
                kept.push(reached);
                continue;
            }
        };
        let StableRecipient::Thread(thread_id) = recipient else {
            continue;
        };
        let Some(camp) = cairn_db::storage::camps::camp_by_id_conn(conn, &camp_id).await? else {
            continue;
        };
        if camp.is_archived() {
            continue;
        }
        let member = CampSubject::Thread {
            thread_id: thread_id.clone(),
        };
        if !cairn_db::storage::camps::is_camp_member_conn(conn, &camp, &member).await? {
            continue;
        }
        let (reason, level) = camp_reason(&camp, reached.level);
        kept.push(Reached {
            reason,
            level: level.min(reached.level),
        });
    }
    Ok(kept)
}

/// The stable recipient a job is: its thread when it is that thread's
/// session, the job itself otherwise (and for a job that no longer exists).
pub async fn stable_recipient_for_job_conn(
    conn: &TrackedConnection,
    job_id: &str,
) -> DbResult<StableRecipient> {
    let mut rows = conn
        .query(
            format!(
                "SELECT j.thread_id FROM jobs j WHERE j.id = ?1 AND j.thread_id IS NOT NULL AND {} LIMIT 1",
                cairn_db::lineage::SESSION_JOB_SHAPE
            ),
            params![job_id],
        )
        .await?;
    Ok(match rows.next().await? {
        Some(row) => StableRecipient::Thread(row.text(0)?),
        None => StableRecipient::Job(job_id.to_string()),
    })
}

/// [`stable_recipient_for_job_conn`] as its own read.
pub async fn stable_recipient_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<StableRecipient, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move { stable_recipient_for_job_conn(conn, &job_id).await })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The job a delivery to `recipient` lands on now, or why there is none.
///
/// A thread resolves to its newest session, and, when it has none and
/// `establish` is set, to a session established in this transaction (the
/// same launch path a direct message takes); a passive line never creates a
/// session to ride along with. A closed thread is not deliverable, and
/// closure never resurrects it. A job resolves to itself unless it is gone or
/// its execution has been superseded.
pub async fn deliverable_job_conn(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    conn: &TrackedConnection,
    recipient: &StableRecipient,
    establish: bool,
) -> DbResult<Result<String, Undeliverable>> {
    match recipient {
        StableRecipient::Thread(thread_id) => {
            let active = cairn_db::lineage::thread_status_conn(conn, thread_id)
                .await?
                .is_some_and(|status| status.is_active());
            if !active {
                return Ok(Err(Undeliverable::ThreadClosed));
            }
            match thread_session_conn(conn, thread_id).await? {
                Some(job_id) => Ok(Ok(job_id)),
                None if establish => host.ensure_thread_session(conn, thread_id).await.map(Ok),
                None => Ok(Err(Undeliverable::NoSessionToRideAlong)),
            }
        }
        StableRecipient::Job(job_id) => {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM jobs WHERE id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            if rows.next().await?.is_none() {
                return Ok(Err(Undeliverable::JobGone));
            }
            drop(rows);
            if host.job_is_fenced(conn, job_id).await? {
                return Ok(Err(Undeliverable::Superseded));
            }
            Ok(Ok(job_id.clone()))
        }
    }
}

/// Why a recipient has no job a delivery can land on now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Undeliverable {
    /// The thread is closed; it may reopen.
    ThreadClosed,
    /// A passive line has no session to ride along with; one may start.
    NoSessionToRideAlong,
    /// The job no longer exists.
    JobGone,
    /// The job's execution was superseded; it never becomes canonical again.
    Superseded,
}

impl Undeliverable {
    pub fn describe(self) -> &'static str {
        match self {
            Self::ThreadClosed => "the thread is closed",
            Self::NoSessionToRideAlong => "the thread has no session to ride along with",
            Self::JobGone => "the job no longer exists",
            Self::Superseded => "the job's execution was superseded",
        }
    }

    /// Whether the refusal can lift, so a receipt that waits out a
    /// reversible refusal may be held rather than dropped.
    pub fn is_reversible(self) -> bool {
        matches!(self, Self::ThreadClosed | Self::NoSessionToRideAlong)
    }
}

/// The newest session job of a thread, if it has one.
pub async fn thread_session_conn(
    conn: &TrackedConnection,
    thread_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            format!(
                "SELECT j.id FROM jobs j WHERE j.thread_id = ?1 AND {}
                 ORDER BY j.created_at DESC, j.rowid DESC LIMIT 1",
                cairn_db::lineage::SESSION_JOB_SHAPE
            ),
            params![thread_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.text(0)?)),
        None => Ok(None),
    }
}

async fn job_issue_conn(conn: &TrackedConnection, job_id: &str) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT issue_id FROM jobs WHERE id = ?1 LIMIT 1",
            params![job_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => row.opt_text(0),
        None => Ok(None),
    }
}

/// The project a recipient belongs to: a thread's own project, or a job's
/// issue project falling back to the job's.
async fn recipient_project_conn(
    conn: &TrackedConnection,
    recipient: &StableRecipient,
) -> DbResult<Option<String>> {
    let mut rows = match recipient {
        StableRecipient::Thread(id) => {
            conn.query(
                "SELECT project_id FROM threads WHERE id = ?1 LIMIT 1",
                params![id.as_str()],
            )
            .await?
        }
        StableRecipient::Job(id) => {
            conn.query(
                "SELECT COALESCE(i.project_id, j.project_id) FROM jobs j
                   LEFT JOIN issues i ON i.id = j.issue_id
                  WHERE j.id = ?1 LIMIT 1",
                params![id.as_str()],
            )
            .await?
        }
    };
    match rows.next().await? {
        Some(row) => row.opt_text(0),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delivery::{push_to_audience, AudiencePush, PushContent, PushGuard};
    use crate::push::Boundary;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct Host;
    #[async_trait]
    impl crate::runtime::RecipientHost for Host {
        async fn ensure_thread_session(
            &self,
            _: &TrackedConnection,
            thread_id: &str,
        ) -> DbResult<String> {
            Ok(format!("{thread_id}-session"))
        }
        async fn job_is_fenced(&self, _: &TrackedConnection, _: &str) -> DbResult<bool> {
            Ok(false)
        }
        async fn job_issue_is_resolved(&self, _: &TrackedConnection, _: &str) -> DbResult<bool> {
            Ok(false)
        }
    }

    const CHILD: &str = "cairn://p/proj/2";

    /// A coordinator on the parent issue, a child issue, camp `work` with
    /// members `lead`, `waker` and the operator, and base camp, whose members
    /// are the open threads `lead`, `waker` and `quiet` (`gone` is closed).
    async fn fixture() -> LocalDb {
        let db = cairn_db::storage::migrated_test_db("recipients.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','proj','/tmp',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('i1','p',1,'Parent','active','active','none',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,current_session_id,started_at,created_at,updated_at) VALUES('coord','p','i1','running','s',1,1,1);
             INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) VALUES('lead','p','lead','active','none',1,1),('waker','p','waker','active','none',1,1),('quiet','p','quiet','active','none',1,1),('gone','p','gone','closed','none',1,1);
             INSERT INTO jobs(id,project_id,thread_id,uri_segment,status,created_at,updated_at) VALUES('lead-session','p','lead','thread','idle',1,1),('waker-session','p','waker','thread','idle',1,1),('quiet-session','p','quiet','thread','idle',1,1);
             INSERT INTO camps(id,project_id,slug,title,kind,status,created_at,updated_at) VALUES('c1','p','work','Work','camp','active',1,1),('b','p','base','Base camp','base','active',1,1);
             INSERT INTO wake_subscriptions(id,source_kind,camp_id,subject_key,subject_json,created_at,updated_at) VALUES('cl','camp','c1','lead','{}',1,1),('cw','camp','c1','waker','{}',1,1),('co','camp','c1','operator','{}',1,1),('cg','camp','c1','gone','{}',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,parent_issue_id,created_at,updated_at) VALUES('i2','p',2,'Child','active','active','none','i1',2,2);",
        )
        .await
        .unwrap();
        db
    }

    fn audience(fact_kind: &str) -> FactAudience {
        let mut audience = FactAudience::new(fact_kind, Wake::Wake, Wake::Wake);
        audience.source = Some(SourceRef::issue(CHILD));
        audience
    }

    async fn resolve(db: &LocalDb, audience: &FactAudience) -> Vec<(StableRecipient, Wake)> {
        resolve_recipients(Arc::new(Host), db, audience)
            .await
            .unwrap()
            .into_iter()
            .map(|candidate| (candidate.recipient, candidate.level))
            .collect()
    }

    fn thread(id: &str) -> StableRecipient {
        StableRecipient::Thread(id.into())
    }

    fn addressing(camp: &str) -> FactAudience {
        let mut audience = audience("question");
        audience.camps.push(camp.into());
        audience
    }

    #[tokio::test]
    async fn a_camp_wakes_its_members_and_base_camp_reaches_every_open_thread_passively() {
        let db = fixture().await;
        let coord = (StableRecipient::Job("coord".into()), Wake::Wake);
        assert_eq!(
            resolve(&db, &addressing("c1")).await,
            vec![
                (thread("lead"), Wake::Wake),
                (thread("waker"), Wake::Wake),
                coord.clone(),
            ],
            "the operator and a closed thread receive nothing"
        );
        assert_eq!(
            resolve(&db, &addressing("b")).await,
            vec![
                (thread("lead"), Wake::Passive),
                (thread("quiet"), Wake::Passive),
                (thread("waker"), Wake::Passive),
                coord.clone(),
            ]
        );

        db.execute(
            "UPDATE camps SET status='archived', archived_at=2 WHERE id='c1'",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            resolve(&db, &addressing("c1")).await,
            vec![coord],
            "an archived camp reaches no one; the parent edge still delivers"
        );
    }

    #[tokio::test]
    async fn a_queued_camp_reason_is_withdrawn_once_its_member_or_camp_is_gone() {
        let db = fixture().await;
        let reached = |camp: &str| Reached {
            reason: Reason::Camp { camp: camp.into() },
            level: Wake::Wake,
        };
        let revalidate = |recipient: &'static str, reasons: Vec<Reached>| {
            let db = &db;
            async move {
                db.read(move |conn| {
                    let reasons = reasons.clone();
                    Box::pin(async move {
                        revalidate_reasons_conn(conn, &thread(recipient), reasons).await
                    })
                })
                .await
                .unwrap()
            }
        };
        let kept = revalidate("waker", vec![reached("c1"), reached("b")]).await;
        assert_eq!(
            kept.iter().map(|r| r.level).collect::<Vec<_>>(),
            vec![Wake::Wake, Wake::Passive],
            "base camp re-levels to passive at delivery"
        );
        assert!(revalidate("quiet", vec![reached("c1")]).await.is_empty());
        db.execute(
            "UPDATE camps SET status='archived', archived_at=2 WHERE id='c1'",
            (),
        )
        .await
        .unwrap();
        assert!(revalidate("waker", vec![reached("c1")]).await.is_empty());
    }

    /// A receipt queued before cairn/7002, rewritten by its migration, keeps
    /// the role field it was stored with; it still reads as a camp reason.
    #[test]
    fn a_migrated_camp_reason_reads_with_its_leftover_role() {
        let stored = r#"[{"kind":"camp","camp":"cb","role":"watch","level":"passive"}]"#;
        let reasons: Vec<Reached> = serde_json::from_str(stored).unwrap();
        assert_eq!(
            reasons,
            vec![Reached {
                reason: Reason::Camp { camp: "cb".into() },
                level: Wake::Passive,
            }]
        );
    }

    #[tokio::test]
    async fn one_recipient_keeps_the_loudest_level_and_every_reason() {
        let db = fixture().await;
        // A base camp member is also addressed directly, and mutes the issue.
        db.execute_script(
            "INSERT INTO wake_subscriptions(id,job_id,source_kind,source_ref,state,created_by,created_at,updated_at)
               VALUES('mute','quiet-session','issue','cairn://p/proj/2','muted','agent',1,1);",
        )
        .await
        .unwrap();
        let mut audience = addressing("b");
        audience.offered.push((
            thread("quiet"),
            Reason::Addressed {
                field: "mentions".into(),
            },
            Wake::Wake,
        ));
        let quiet = resolve_recipients(Arc::new(Host), &db, &audience)
            .await
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.recipient == thread("quiet"))
            .unwrap();
        assert_eq!(
            quiet.level,
            Wake::Wake,
            "a passive camp line or a mute never lowers a wake"
        );
        let reasons: Vec<_> = quiet
            .reasons
            .iter()
            .map(|reached| (reached.reason.clone(), reached.level))
            .collect();
        assert_eq!(
            reasons,
            vec![
                (
                    Reason::Subscription {
                        id: "mute".into(),
                        muted: true
                    },
                    Wake::Passive
                ),
                (Reason::Camp { camp: "b".into() }, Wake::Passive),
                (
                    Reason::Addressed {
                        field: "mentions".into()
                    },
                    Wake::Wake
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_subscription_hears_only_the_fact_kinds_it_names() {
        let db = fixture().await;
        db.execute_script(
            "INSERT INTO jobs(id,project_id,issue_id,status,created_at,updated_at) VALUES('reviewer','p','i1','running',1,1);
             INSERT INTO wake_subscriptions(id,job_id,source_kind,source_ref,fact_kinds_json,state,created_by,created_at,updated_at)
               VALUES('narrow','reviewer','issue','cairn://p/proj/2','[\"review\"]','active','agent',1,1);",
        )
        .await
        .unwrap();
        let reviewer = StableRecipient::Job("reviewer".into());
        let heard = |kind: &'static str| {
            let db = &db;
            let reviewer = reviewer.clone();
            async move {
                resolve(db, &audience(kind))
                    .await
                    .iter()
                    .any(|(recipient, _)| *recipient == reviewer)
            }
        };
        assert!(heard("review").await);
        assert!(!heard("question").await);
    }

    fn push(fact_key: &str) -> AudiencePush {
        AudiencePush {
            fact_key: fact_key.into(),
            content: PushContent::Fixed(CHILD.into()),
            boundary: Boundary::Event,
            key: format!("resolved:{CHILD}"),
            fingerprint: Some(fact_key.into()),
            guard: PushGuard::None,
        }
    }

    async fn pushes(db: &LocalDb) -> i64 {
        db.query_one("SELECT COUNT(*) FROM attention_pushes", (), |row| {
            row.i64(0)
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_retried_push_delivers_nothing_and_a_recurrence_delivers_again() {
        let db = fixture().await;
        let mut audience = audience("resolved");
        audience.camps.push("c1".into());
        let first = push_to_audience(Arc::new(Host), &db, &audience, &push("resolved#a"))
            .await
            .unwrap();
        assert_eq!(first.len(), 3);
        assert!(first.iter().all(|delivery| delivery.fresh));
        assert_eq!(pushes(&db).await, 3);

        let retry = push_to_audience(Arc::new(Host), &db, &audience, &push("resolved#a"))
            .await
            .unwrap();
        assert!(retry.iter().all(|delivery| !delivery.fresh));
        assert_eq!(pushes(&db).await, 3, "the same fact is never pushed twice");

        let again = push_to_audience(Arc::new(Host), &db, &audience, &push("resolved#b"))
            .await
            .unwrap();
        assert!(again.iter().all(|delivery| delivery.fresh));
    }

    #[test]
    fn a_deferral_admits_at_its_deadline_and_never_later() {
        use crate::wakes::{admit_wake, deferral_recheck_at, Admission, DeferUntil};
        let defer = DeferUntil {
            deadline_ms: 1_000,
            reason: "never clears".into(),
        };
        assert_eq!(
            admit_wake(999, [Some(defer.clone())]),
            Admission::Defer(defer.clone())
        );
        assert_eq!(admit_wake(1_000, [Some(defer.clone())]), Admission::Admit);
        assert_eq!(deferral_recheck_at(990, &defer), 1_000);
        assert_eq!(admit_wake(0, [None]), Admission::Admit);
    }

    #[test]
    fn a_delivery_message_id_names_its_recipient_and_its_sorted_facts() {
        let id = |keys: &[&str]| crate::wakes::delivery_message_id(&thread("t"), keys);
        assert_eq!(id(&["b", "a"]), id(&["a", "b"]));
        assert_ne!(id(&["a", "b"]), id(&["a", "b", "c"]));
        assert_ne!(
            id(&["a"]),
            crate::wakes::delivery_message_id(&thread("u"), &["a"])
        );
    }
}
