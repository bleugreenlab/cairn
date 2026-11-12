//! The rebuild wake: `{subscribe:{kind:"rebuild", contains:<commit | issue>}}`.
//!
//! Much of what a thread owes is "verify this live once the runner is running a
//! build that has it". This subscription turns that from something the thread
//! has to remember into something that finds it: it fires once, at the first
//! runner boot whose recorded source commit contains the target by store
//! ancestry, and then it is spent.
//!
//! **The row.** The agent-facing kind compiles, the way `kind:"posts"` does, to
//! one `fact` subscription whose clause is
//! `{fact: runner_booted, contains: <target>, project: <key>}`, stored one-shot.
//! The clause is the subscription's identity, so subscribe, mute, unmute and
//! unsubscribe address one row. Matching is NOT the fact log's: "contains by
//! ancestry" is not a field predicate its index lane can evaluate, so the
//! evaluator here reads each pending clause and asks the project's store.
//!
//! **A firing is its own fact, delivered as a receipt.** When a boot contains
//! the target, the evaluator spends the subscription and writes one delivery
//! receipt, `rebuild-wake:<subscription id>`, in one transaction; the fact
//! log's drain delivers it like every other receipt (camp design, cairn/6255
//! §3). The key is unique because the subscription is one-shot, so a later
//! boot finds nothing pending and a retry is refused by the receipt itself.
//! The firing cannot simply join the boot's base-camp rebuild fact: that fact
//! exists only for a boot that changed builds, and a base camp member may
//! already have read it passively. So the firing's receipt *references* the
//! camp fact's row when the boot has one for the subscriber's project, and
//! the drain carries a recipient's receipts for one row in one message at
//! their loudest level, framed `From camp base: …`. With no camp fact it
//! renders the firing's own words
//! from the facts its receipt keeps (`fact_json`).
//!
//! A recipient that cannot take the wake now (a closed thread, a resolved
//! issue) keeps the receipt queued, listed on its `/wakes`, and receives it
//! once it can; a superseded execution's receipt is dropped. The firing never
//! re-arms a subscription a later boot could fire again.
//!
//! **The producer** is the runner's boot. The host hands core its boot facts
//! once ([`record_current_boot`]); the pending pass
//! ([`evaluate_pending_rebuild_wakes`]) runs at the end of the startup
//! default-advance chain, after the store is open and the base-advance catch-up
//! has brought each base branch current, so the ancestry read can see the
//! commit the build came from. Boot-time firings are paced by the drain: each
//! one's wake comes due a spacing after the previous, so a boot with many
//! waiting threads does not launch them all in one burst. A project whose
//! subscription this boot fires is told about the rebuild even when it sits
//! outside the announcement's activity window, so its subscriber's line is
//! the camp's.
//!
//! **Unknown is not a match.** A build with no embedded source commit never
//! satisfies a subscription; it waits for a stamped boot. An issue target that
//! has not merged resolves when it merges, and waits until then.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::Duration;

use cairn_attention::push::Wake;
use cairn_attention::wakes::{Candidate, NewReceipt, Reached, Reason, ReceiptWrite};
use cairn_db::turso::params;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::orchestrator::build_change::BuildChange;
use crate::orchestrator::Orchestrator;
use crate::storage::{RowExt, WakeMatch, WakeWindow};

use super::store::{fact_clause_ref, subscribe_one_shot};
use super::types::*;

/// The fact a rebuild subscription's clause names.
pub const RUNNER_BOOTED: &str = "runner_booted";

/// What a firing's receipt key opens with; the subscription id follows.
pub const REBUILD_WAKE_KEY_PREFIX: &str = "rebuild-wake:";

/// The wake source a firing rides under when its boot has no camp fact for
/// the subscriber's project to ride beside.
pub const REBUILD_WAKE_SOURCE: &str = "rebuild_wake";

/// The row id a firing's wake stores when its boot has no camp fact: no row.
/// `fact_log_wakes.first_row_id` is NOT NULL, so the firing's own wake source
/// ([`REBUILD_WAKE_SOURCE`]) holds this sentinel; the drain renders such a
/// wake from its receipt alone and never reads it as a row (pinned in the
/// rebuild tests).
pub const NO_FACT_ROW: i64 = 0;

/// The receipt key of one subscription's firing.
pub fn rebuild_wake_key(subscription_id: &str) -> String {
    format!("{REBUILD_WAKE_KEY_PREFIX}{subscription_id}")
}

/// The spacing between two boot-time firings' wakes. Each delivery can launch
/// a thread's turn, and a rebuild is exactly when many threads are waiting.
const BOOT_DELIVERY_SPACING: Duration = Duration::from_secs(3);

/// The window a firing's wake is folded with: due at once, and never pulled
/// earlier than the camp wake it may join was already due.
const FIRING_WINDOW: WakeWindow = WakeWindow {
    settle_ms: 0,
    max_window_ms: 60_000,
};

/// The facts of one runner boot a rebuild wake reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerBoot {
    pub booted_at: i64,
    pub version: String,
    pub build_id: String,
    /// The full commit the build was made from; `None` for an unstamped build.
    pub source_commit: Option<String>,
    /// The rebuild notice this boot owes base camps, when it changed builds
    /// (or an earlier boot's notice is still owed). Its camp facts are what a
    /// firing on this boot rides beside.
    pub build_change: Option<BuildChange>,
}

static CURRENT_BOOT: OnceLock<RunnerBoot> = OnceLock::new();

/// Record this process's boot. One runner process is one boot, so the first
/// call wins and a second is a host bug, logged rather than obeyed.
pub fn record_current_boot(boot: RunnerBoot) {
    if CURRENT_BOOT.set(boot).is_err() {
        log::warn!("runner boot already recorded for this process; ignoring a second record");
    }
}

/// This process's boot, once the host has recorded it.
pub fn current_boot() -> Option<&'static RunnerBoot> {
    CURRENT_BOOT.get()
}

/// What a rebuild wake waits to see a boot contain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebuildTarget {
    /// A full 40-character commit id, lowercase.
    Commit(String),
    /// An issue, which resolves to the commit its merge wrote.
    Issue { project: String, number: i32 },
}

impl RebuildTarget {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.len() == 40 && raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Ok(Self::Commit(raw.to_ascii_lowercase()));
        }
        match cairn_uri::parse_uri(raw) {
            Some(cairn_uri::CairnResource::Issue { project, number }) => Ok(Self::Issue {
                project: cairn_uri::canonical_project(&project),
                number,
            }),
            _ => Err(format!(
                "a rebuild wake's contains must be a full 40-character commit id or an issue URI (cairn://p/<project>/<number>); got '{raw}'"
            )),
        }
    }

    /// The canonical spelling the clause stores.
    pub fn canonical(&self) -> String {
        match self {
            Self::Commit(commit) => commit.clone(),
            Self::Issue { project, number } => format!("cairn://p/{project}/{number}"),
        }
    }

    /// The project whose store answers the ancestry question: an issue's own,
    /// otherwise the subscriber's.
    fn store_project(&self, subscriber_project: &str) -> String {
        match self {
            Self::Issue { project, .. } => project.clone(),
            Self::Commit(_) => cairn_uri::canonical_project(subscriber_project),
        }
    }
}

impl std::fmt::Display for RebuildTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.canonical())
    }
}

/// A compiled rebuild subscription: what it waits for and whose store answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebuildClause {
    pub target: RebuildTarget,
    pub project: String,
}

impl RebuildClause {
    pub fn new(target: RebuildTarget, subscriber_project: &str) -> Self {
        let project = target.store_project(subscriber_project);
        Self { target, project }
    }

    /// The `fact` subscription reference this clause is stored and delivered
    /// under.
    pub fn clause_ref(&self) -> Result<String, String> {
        fact_clause_ref(&BTreeMap::from([
            ("fact".to_string(), Value::String(RUNNER_BOOTED.to_string())),
            (
                "contains".to_string(),
                Value::String(self.target.canonical()),
            ),
            ("project".to_string(), Value::String(self.project.clone())),
        ]))
    }

    /// Read a stored subscription back as a rebuild clause; `None` for any
    /// other subscription.
    pub fn from_subscription(subscription: &WakeSubscription) -> Option<Self> {
        if subscription.source_kind != SOURCE_KIND_FACT
            || subscription.fact_kinds.as_deref() != Some(&[RUNNER_BOOTED.to_string()][..])
        {
            return None;
        }
        let clause: BTreeMap<String, Value> =
            serde_json::from_str(subscription.source_ref.as_deref()?).ok()?;
        if clause.get("fact").and_then(Value::as_str) != Some(RUNNER_BOOTED) {
            return None;
        }
        let target = RebuildTarget::parse(clause.get("contains")?.as_str()?).ok()?;
        let project = clause.get("project")?.as_str()?.to_string();
        Some(Self { target, project })
    }
}

/// The ancestry question a boot is judged by. Production asks the project's
/// jj store ([`StoreContainment`]); a test answers from a table.
pub trait Containment: Send + Sync {
    /// Whether `boot_commit` contains `target_commit` in `project`'s store.
    fn contains<'a>(
        &'a self,
        project: &'a str,
        target_commit: &'a str,
        boot_commit: &'a str,
    ) -> BoxFuture<'a, Result<bool, String>>;
}

/// The answer `cairn:@/vcs/ancestry` gives: the project's runner-owned store.
pub struct StoreContainment<'a> {
    orch: &'a Orchestrator,
}

impl<'a> StoreContainment<'a> {
    pub fn new(orch: &'a Orchestrator) -> Self {
        Self { orch }
    }
}

impl Containment for StoreContainment<'_> {
    fn contains<'a>(
        &'a self,
        project: &'a str,
        target_commit: &'a str,
        boot_commit: &'a str,
    ) -> BoxFuture<'a, Result<bool, String>> {
        Box::pin(async move {
            let db = self.orch.db.for_project(project).await;
            let project_id =
                crate::mcp::handlers::run_context::project_id_by_key(&db, project).await?;
            let store = crate::mcp::handlers::branch::resolve_for_project_read(
                self.orch,
                &project_id,
                "main",
            )
            .await?
            .repository_path;
            cairn_vcs::history::contains(&store, target_commit, boot_commit)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

/// Why a rebuild subscription did not fire on a given boot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebuildWait {
    /// The boot's build carries no source commit, so it can never satisfy one.
    UnknownBootCommit,
    /// The issue target has not merged, or its merge commit was not recorded.
    IssueNotMerged,
    /// The boot's commit does not contain the target.
    NotContained { boot_commit: String },
    /// The store could not answer (an unknown commit, an unreadable store).
    AncestryUnavailable(String),
}

impl RebuildWait {
    fn describe(&self) -> String {
        match self {
            Self::UnknownBootCommit => "the running build records no source commit, so it cannot contain anything; waiting for a stamped boot".to_string(),
            Self::IssueNotMerged => "the issue has not merged yet; it resolves to its merge commit when it does".to_string(),
            Self::NotContained { boot_commit } => format!("the running build ({}) does not contain it yet", short(boot_commit)),
            Self::AncestryUnavailable(error) => format!("the store could not answer whether the running build contains it ({error}); the next boot asks again"),
        }
    }
}

/// One subscription judged against one boot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebuildEvaluation {
    /// The boot contains the target: the subscription is spent and its
    /// firing's receipt is queued for the drain.
    Fired,
    /// The boot contains the target, but the subscription was already spent
    /// or its firing already has a receipt; nothing more is owed.
    AlreadyFired,
    Waiting(RebuildWait),
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// The commit an issue's merge wrote, when it has merged.
async fn issue_merge_commit(
    orch: &Orchestrator,
    project: &str,
    number: i32,
) -> Result<Option<String>, String> {
    orch.db
        .for_project(project)
        .await
        .query_opt_text(
            "SELECT mr.merged_commit FROM merge_requests mr
               JOIN issues i ON i.id = mr.issue_id
               JOIN projects p ON p.id = i.project_id
              WHERE lower(p.key) = lower(?1) AND i.number = ?2
                AND mr.status = 'merged' AND mr.merged_commit IS NOT NULL
              ORDER BY COALESCE(mr.merged_at, mr.updated_at) DESC
              LIMIT 1",
            params![project, i64::from(number)],
        )
        .await
        .map_err(|error| format!("could not read {project}/{number}'s merge: {error}"))
}

async fn issue_exists(orch: &Orchestrator, project: &str, number: i32) -> Result<bool, String> {
    orch.db
        .for_project(project)
        .await
        .query_opt_i64(
            "SELECT 1 FROM issues i JOIN projects p ON p.id = i.project_id
              WHERE lower(p.key) = lower(?1) AND i.number = ?2 LIMIT 1",
            params![project, i64::from(number)],
        )
        .await
        .map(|row| row.is_some())
        .map_err(|error| format!("could not read {project}/{number}: {error}"))
}

/// What one firing found: the facts its message reports. Kept on its receipt
/// (`fact_json`), because the spent subscription is gone by delivery and a
/// restart may deliver it under a later boot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RebuildFiring {
    /// The clause's target, canonical: a commit or an issue URI.
    pub target: String,
    /// The commit the target resolved to (an issue's merge commit).
    pub target_commit: String,
    pub boot_commit: String,
    pub booted_at: i64,
    pub version: String,
    pub build_id: String,
    /// Whether it fired as it was subscribed, the current boot already
    /// containing the target.
    pub at_subscribe: bool,
}

impl RebuildFiring {
    /// The firing in its own words, for a boot with no camp fact to frame it.
    pub fn render(&self) -> String {
        let booted =
            crate::clock::stamp(self.booted_at).unwrap_or_else(|| self.booted_at.to_string());
        let target = if self.target == self.target_commit {
            format!("`{}`", self.target_commit)
        } else {
            format!("{} (merged as `{}`)", self.target, self.target_commit)
        };
        let headline = if self.at_subscribe {
            format!(
                "[Rebuild] The runner's current boot already contains {target}, so this wake fired at once instead of waiting for another rebuild."
            )
        } else {
            format!("[Rebuild] The runner booted with a build that contains {target}.")
        };
        format!(
            "{headline}\n\n- Booted: {booted}\n- Version: {}\n- Build: {}\n- Source commit: `{}`\n\nThe subscription is spent; subscribe again to wait for a later commit.",
            self.version, self.build_id, self.boot_commit
        )
    }
}

/// The words a firing's stored facts render as; `None` when they do not read.
pub fn render_rebuild_firing(fact_json: &str) -> Option<String> {
    serde_json::from_str::<RebuildFiring>(fact_json)
        .ok()
        .map(|firing| firing.render())
}

/// Whether `boot` contains `clause`'s target, and what the firing reports
/// when it does.
async fn judge(
    orch: &Orchestrator,
    boot: &RunnerBoot,
    containment: &dyn Containment,
    clause: &RebuildClause,
    at_subscribe: bool,
) -> Result<Result<RebuildFiring, RebuildWait>, String> {
    let Some(boot_commit) = boot.source_commit.as_deref() else {
        return Ok(Err(RebuildWait::UnknownBootCommit));
    };
    let target_commit = match &clause.target {
        RebuildTarget::Commit(commit) => commit.clone(),
        RebuildTarget::Issue { project, number } => {
            match issue_merge_commit(orch, project, *number).await? {
                Some(commit) => commit,
                None => return Ok(Err(RebuildWait::IssueNotMerged)),
            }
        }
    };
    match containment
        .contains(&clause.project, &target_commit, boot_commit)
        .await
    {
        Ok(true) => Ok(Ok(RebuildFiring {
            target: clause.target.canonical(),
            target_commit,
            boot_commit: boot_commit.to_string(),
            booted_at: boot.booted_at,
            version: boot.version.clone(),
            build_id: boot.build_id.clone(),
            at_subscribe,
        })),
        Ok(false) => Ok(Err(RebuildWait::NotContained {
            boot_commit: boot_commit.to_string(),
        })),
        Err(error) => Ok(Err(RebuildWait::AncestryUnavailable(error))),
    }
}

/// The project a subscriber job belongs to, as (id, key).
async fn subscriber_project(orch: &Orchestrator, job_id: &str) -> Option<(String, String)> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .ok()?;
    db.query_opt(
        "SELECT p.id, p.key FROM jobs j JOIN projects p ON p.id = j.project_id WHERE j.id = ?1",
        params![job_id],
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .ok()
    .flatten()
}

/// The row of `boot`'s base-camp rebuild fact for a project, if the boot owes
/// one. `mint` tells a project the announcement passed over (one outside its
/// activity window) now, so a firing on this boot rides beside its camp fact.
async fn boot_camp_fact(
    orch: &Orchestrator,
    boot: &RunnerBoot,
    project_id: &str,
    project_key: &str,
    mint: bool,
) -> Option<i64> {
    let change = boot.build_change.as_ref()?;
    let identity = crate::orchestrator::build_change::notice_identity(change, project_id);
    let find = || async {
        orch.db
            .local
            .query_opt_i64(
                "SELECT id FROM fact_log WHERE source = ?1 AND identity = ?2 LIMIT 1",
                params![crate::camps::CAMP_NOTICE, identity.as_str()],
            )
            .await
            .ok()
            .flatten()
    };
    if let Some(id) = find().await {
        return Some(id);
    }
    if !mint {
        return None;
    }
    match crate::orchestrator::build_change::announce_project(orch, change, project_id, project_key)
        .await
    {
        Ok(Some(id)) => Some(id),
        Ok(None) => find().await,
        Err(error) => {
            log::warn!("could not tell {project_key}'s base camp about the rebuild: {error}");
            None
        }
    }
}

/// Spend `subscription` and queue its firing's receipt, in one transaction.
///
/// `not_before_ms` is the drain's pacing: the wake carrying the receipt comes
/// due no earlier. `mint_camp_fact` is set for a boot-time firing, which tells
/// the subscriber's project's base camp about the rebuild if the announcement
/// passed it over.
async fn fire(
    orch: &Orchestrator,
    boot: &RunnerBoot,
    subscription: &WakeSubscription,
    firing: &RebuildFiring,
    not_before_ms: Option<i64>,
    mint_camp_fact: bool,
) -> Result<RebuildEvaluation, String> {
    let db = &orch.db.local;
    let recipient =
        cairn_attention::wakes::stable_recipient_for_job(db, &subscription.job_id).await?;
    let project = subscriber_project(orch, &subscription.job_id).await;
    let camp_row = match &project {
        Some((id, key)) => boot_camp_fact(orch, boot, id, key, mint_camp_fact).await,
        None => None,
    };
    let muted = subscription.state == WakeSubscriptionState::Muted;
    let level = if muted { Wake::Passive } else { Wake::Wake };
    let candidate = Candidate {
        recipient,
        level,
        reasons: vec![Reached {
            reason: Reason::Subscription {
                id: subscription.id.clone(),
                muted,
            },
            level,
        }],
    };
    let project_id = project.map(|(id, _)| id);
    let matched = WakeMatch {
        recipient_key: candidate.recipient.key(),
        source: if camp_row.is_some() {
            crate::camps::CAMP_NOTICE.to_string()
        } else {
            REBUILD_WAKE_SOURCE.to_string()
        },
        clause_ref: subscription.source_ref.clone().unwrap_or_default(),
        level: level.as_str().to_string(),
        project_id: project_id.clone(),
        row_id: camp_row.unwrap_or(NO_FACT_ROW),
        at_ms: chrono::Utc::now().timestamp_millis(),
    };
    let fact_key = rebuild_wake_key(&subscription.id);
    let fact_json = serde_json::to_string(firing).map_err(|error| error.to_string())?;
    let subscription_id = subscription.id.clone();
    let written = db
        .write(move |conn| {
            let (subscription_id, fact_key, fact_json, candidate, project_id, matched) = (
                subscription_id.clone(),
                fact_key.clone(),
                fact_json.clone(),
                candidate.clone(),
                project_id.clone(),
                matched.clone(),
            );
            Box::pin(async move {
                let spent = conn
                    .execute(
                        "DELETE FROM wake_subscriptions WHERE id = ?1 AND one_shot = 1",
                        params![subscription_id.as_str()],
                    )
                    .await?;
                if spent == 0 {
                    return Ok(None);
                }
                let new = NewReceipt {
                    fact_key: &fact_key,
                    candidate: &candidate,
                    project_id: project_id.as_deref(),
                    fact_row_id: camp_row,
                    fact_json: Some(&fact_json),
                };
                let (write, _) = cairn_attention::wakes::queue_receipt_conn(
                    conn,
                    &new,
                    &matched,
                    FIRING_WINDOW,
                    not_before_ms,
                )
                .await?;
                Ok(Some(write))
            })
        })
        .await
        .map_err(|error| format!("could not fire rebuild wake {}: {error}", subscription.id))?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "wake_subscriptions", "action": "delete"}),
    );
    Ok(match written {
        Some(ReceiptWrite::Inserted(_)) => {
            orch.fact_log.notify.notify_one();
            RebuildEvaluation::Fired
        }
        Some(_) | None => RebuildEvaluation::AlreadyFired,
    })
}

/// Judge one rebuild subscription against one boot, and fire it when the boot
/// contains its target.
pub async fn evaluate_rebuild_subscription(
    orch: &Orchestrator,
    boot: &RunnerBoot,
    containment: &dyn Containment,
    subscription: &WakeSubscription,
    clause: &RebuildClause,
    at_subscribe: bool,
    not_before_ms: Option<i64>,
) -> Result<RebuildEvaluation, String> {
    match judge(orch, boot, containment, clause, at_subscribe).await? {
        Ok(firing) => {
            fire(
                orch,
                boot,
                subscription,
                &firing,
                not_before_ms,
                !at_subscribe,
            )
            .await
        }
        Err(wait) => Ok(RebuildEvaluation::Waiting(wait)),
    }
}

/// What subscribing did, so the caller can say it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebuildSubscribeOutcome {
    /// The current boot already contains the target; the wake fired now.
    FiredNow,
    /// Armed; a later boot will fire it.
    Waiting(RebuildWait),
    /// No boot was recorded in this process (a host that is not the runner).
    NoBootRecorded,
}

impl RebuildSubscribeOutcome {
    pub fn describe(&self, clause: &RebuildClause) -> String {
        let target = clause.target.canonical();
        match self {
            Self::FiredNow => format!(
                "The runner's current boot already contains {target}; the rebuild wake fired now and is queued for turn end"
            ),
            Self::Waiting(wait) => format!(
                "Subscribed to a rebuild containing {target}: {}. End your turn; it fires once, at the first runner boot whose source commit contains it",
                wait.describe()
            ),
            Self::NoBootRecorded => format!(
                "Subscribed to a rebuild containing {target}. This host recorded no runner boot, so the next runner boot decides"
            ),
        }
    }
}

/// Subscribe a job to the first runner boot that contains `target`, firing at
/// once when the current boot already does.
///
/// Without the immediate check a thread waiting on a commit that is already
/// running would wait for a rebuild that may never carry anything newer. The
/// immediate firing is the boot firing's: the subscription is spent and its
/// receipt queued in one transaction, beside the current boot's camp fact
/// when there is one, and the drain delivers it at wake.
pub async fn subscribe_rebuild_once(
    orch: &Orchestrator,
    subscriber_job_id: &str,
    clause: &RebuildClause,
    created_by: &str,
    boot: Option<&RunnerBoot>,
    containment: &dyn Containment,
) -> Result<RebuildSubscribeOutcome, String> {
    if let RebuildTarget::Issue { project, number } = &clause.target {
        if !issue_exists(orch, project, *number).await? {
            return Err(format!(
                "no issue {} exists to wait for",
                clause.target.canonical()
            ));
        }
    }
    let clause_ref = clause.clause_ref()?;
    let subscription = subscribe_one_shot(
        &orch.db.local,
        subscriber_job_id,
        SOURCE_KIND_FACT,
        Some(&clause_ref),
        Some(&[RUNNER_BOOTED.to_string()]),
        created_by,
    )
    .await?;
    let Some(boot) = boot else {
        return Ok(RebuildSubscribeOutcome::NoBootRecorded);
    };
    Ok(
        match evaluate_rebuild_subscription(
            orch,
            boot,
            containment,
            &subscription,
            clause,
            true,
            None,
        )
        .await?
        {
            RebuildEvaluation::Fired | RebuildEvaluation::AlreadyFired => {
                RebuildSubscribeOutcome::FiredNow
            }
            RebuildEvaluation::Waiting(wait) => RebuildSubscribeOutcome::Waiting(wait),
        },
    )
}

/// Every rebuild subscription still armed, oldest first.
async fn pending_rebuild_subscriptions(
    orch: &Orchestrator,
) -> Result<Vec<WakeSubscription>, String> {
    orch.db
        .local
        .query_all(
            "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                    mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                    one_shot, match_phrase
               FROM wake_subscriptions
              WHERE source_kind = ?1 AND fact_kinds_json = ?2
                AND one_shot = 1 AND state != 'unsubscribed'
              ORDER BY created_at ASC, id ASC",
            params![
                SOURCE_KIND_FACT,
                fact_kinds_json(Some(&[RUNNER_BOOTED.to_string()]))
            ],
            subscription_from_row,
        )
        .await
        .map_err(|error| format!("could not list rebuild wakes: {error}"))
}

/// What one boot pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RebuildPass {
    pub fired: usize,
    pub waiting: usize,
    pub failures: usize,
}

/// Judge every armed rebuild subscription against `boot`, firing those it
/// contains. Each firing's wake comes due `spacing` after the previous one's,
/// so the drain paces the deliveries.
pub async fn evaluate_rebuild_wakes(
    orch: &Orchestrator,
    boot: &RunnerBoot,
    containment: &dyn Containment,
    spacing: Duration,
) -> Result<RebuildPass, String> {
    let mut pass = RebuildPass::default();
    if boot.source_commit.is_none() {
        // Nothing can match; say so once rather than once per subscription.
        pass.waiting = pending_rebuild_subscriptions(orch).await?.len();
        return Ok(pass);
    }
    let start_ms = chrono::Utc::now().timestamp_millis();
    let spacing_ms = i64::try_from(spacing.as_millis()).unwrap_or(i64::MAX);
    for subscription in pending_rebuild_subscriptions(orch).await? {
        let Some(clause) = RebuildClause::from_subscription(&subscription) else {
            log::warn!(
                "rebuild wake {} has a clause this runner cannot read; leaving it",
                subscription.id
            );
            pass.failures += 1;
            continue;
        };
        let paced = i64::try_from(pass.fired).unwrap_or(i64::MAX);
        let not_before = start_ms.saturating_add(spacing_ms.saturating_mul(paced));
        match evaluate_rebuild_subscription(
            orch,
            boot,
            containment,
            &subscription,
            &clause,
            false,
            Some(not_before),
        )
        .await
        {
            Ok(RebuildEvaluation::Fired) => pass.fired += 1,
            Ok(RebuildEvaluation::AlreadyFired) => {}
            Ok(RebuildEvaluation::Waiting(wait)) => {
                if let RebuildWait::AncestryUnavailable(error) = &wait {
                    log::info!(
                        "rebuild wake {} on {}: {error}",
                        subscription.id,
                        clause.target
                    );
                }
                pass.waiting += 1;
            }
            Err(error) => {
                log::warn!(
                    "rebuild wake {} on {}: {error}",
                    subscription.id,
                    clause.target
                );
                pass.failures += 1;
            }
        }
    }
    Ok(pass)
}

/// The runner's boot-time pass over every armed rebuild subscription. Called
/// once the startup catch-up has brought each base branch current.
pub async fn evaluate_pending_rebuild_wakes(orch: &Orchestrator) {
    let Some(boot) = current_boot() else {
        return;
    };
    match evaluate_rebuild_wakes(
        orch,
        boot,
        &StoreContainment::new(orch),
        BOOT_DELIVERY_SPACING,
    )
    .await
    {
        Ok(pass) => log::info!(
            "rebuild wakes for boot source_commit={}: {} fired, {} waiting, {} failed",
            boot.source_commit.as_deref().unwrap_or("unknown"),
            pass.fired,
            pass.waiting,
            pass.failures
        ),
        Err(error) => log::warn!("rebuild wakes were not evaluated this boot: {error}"),
    }
}

/// The `cairn:~/wakes` line for a rebuild subscription; `None` for any other.
pub fn describe_rebuild_subscription(subscription: &WakeSubscription) -> Option<String> {
    let clause = RebuildClause::from_subscription(subscription)?;
    let state = match subscription.state {
        WakeSubscriptionState::Active => "waiting",
        WakeSubscriptionState::Muted => "waiting, muted (fires passively)",
        WakeSubscriptionState::Unsubscribed => "unsubscribed",
    };
    Some(format!(
        "- `{}` rebuild contains `{}` (store: {}) — {state}; one-shot, fires at the first runner boot whose source commit contains it",
        subscription.state.as_str(),
        clause.target.canonical(),
        clause.project
    ))
}
