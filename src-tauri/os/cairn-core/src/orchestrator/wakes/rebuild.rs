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
//! ancestry" is not a field predicate its index lane can evaluate, so the boot
//! evaluator here reads each pending clause, asks the project's store, and
//! delivers a targeted wake through the ordinary router. The router owns
//! everything delivery-shaped: a closed thread is dropped without spending the
//! row, a fenced execution spends it, a muted row rides along passively, and a
//! delivered row is consumed.
//!
//! **The producer** is the runner's boot. The host hands core its boot facts
//! once ([`record_current_boot`]); the pending pass
//! ([`evaluate_pending_rebuild_wakes`]) runs at the end of the startup
//! default-advance chain, after the store is open and the base-advance catch-up
//! has brought each base branch current, so the ancestry read can see the
//! commit the build came from. It delivers serially with a spacing, so a boot
//! with many waiting threads does not launch them all in one burst.
//!
//! **Unknown is not a match.** A build with no embedded source commit never
//! satisfies a subscription; it waits for a stamped boot. An issue target that
//! has not merged resolves when it merges, and waits until then.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::Duration;

use cairn_db::turso::params;
use futures_util::future::BoxFuture;
use serde_json::Value;

use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;

use super::routing::route_wake;
use super::store::{fact_clause_ref, subscribe_one_shot};
use super::types::*;

/// The fact a rebuild subscription's clause names.
pub const RUNNER_BOOTED: &str = "runner_booted";

/// Pause between two boot-time rebuild deliveries. Each delivery can launch a
/// thread's turn, and a rebuild is exactly when many threads are waiting.
const BOOT_DELIVERY_SPACING: Duration = Duration::from_secs(3);

/// The facts of one runner boot a rebuild wake reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerBoot {
    pub booted_at: i64,
    pub version: String,
    pub build_id: String,
    /// The full commit the build was made from; `None` for an unstamped build.
    pub source_commit: Option<String>,
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
    /// Routed: the router's verdict on the delivery.
    Routed(WakeRouteAction),
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

fn format_rebuild_message(
    clause: &RebuildClause,
    target_commit: &str,
    boot: &RunnerBoot,
    boot_commit: &str,
    at_subscribe: bool,
) -> String {
    let booted = crate::clock::stamp(boot.booted_at).unwrap_or_else(|| boot.booted_at.to_string());
    let target = match &clause.target {
        RebuildTarget::Commit(_) => format!("`{target_commit}`"),
        RebuildTarget::Issue { .. } => {
            format!(
                "{} (merged as `{target_commit}`)",
                clause.target.canonical()
            )
        }
    };
    let headline = if at_subscribe {
        format!(
            "[Rebuild] The runner's current boot already contains {target}, so this wake fired at once instead of waiting for another rebuild."
        )
    } else {
        format!("[Rebuild] The runner booted with a build that contains {target}.")
    };
    format!(
        "{headline}\n\n- Booted: {booted}\n- Version: {}\n- Build: {}\n- Source commit: `{boot_commit}`\n\nThe subscription is spent; subscribe again to wait for a later commit.",
        boot.version, boot.build_id
    )
}

/// Judge one rebuild subscription against one boot, and deliver it when the
/// boot contains its target.
pub async fn evaluate_rebuild_subscription(
    orch: &Orchestrator,
    boot: &RunnerBoot,
    containment: &dyn Containment,
    subscription: &WakeSubscription,
    clause: &RebuildClause,
    at_subscribe: bool,
) -> Result<RebuildEvaluation, String> {
    let Some(boot_commit) = boot.source_commit.as_deref() else {
        return Ok(RebuildEvaluation::Waiting(RebuildWait::UnknownBootCommit));
    };
    let target_commit = match &clause.target {
        RebuildTarget::Commit(commit) => commit.clone(),
        RebuildTarget::Issue { project, number } => {
            match issue_merge_commit(orch, project, *number).await? {
                Some(commit) => commit,
                None => return Ok(RebuildEvaluation::Waiting(RebuildWait::IssueNotMerged)),
            }
        }
    };
    match containment
        .contains(&clause.project, &target_commit, boot_commit)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            return Ok(RebuildEvaluation::Waiting(RebuildWait::NotContained {
                boot_commit: boot_commit.to_string(),
            }))
        }
        Err(error) => {
            return Ok(RebuildEvaluation::Waiting(
                RebuildWait::AncestryUnavailable(error),
            ))
        }
    }
    let clause_ref = subscription
        .source_ref
        .clone()
        .ok_or("a rebuild subscription has no clause")?;
    let action = route_wake(
        orch,
        WakeEvent {
            source: WakeSource::Fact { clause_ref },
            fact_kind: RUNNER_BOOTED.to_string(),
            detail_uri: match &clause.target {
                RebuildTarget::Issue { .. } => Some(clause.target.canonical()),
                RebuildTarget::Commit(_) => None,
            },
            delivery: WakeDelivery::Targeted {
                subscriber_job_id: subscription.job_id.clone(),
                sender_name: None,
                message: format_rebuild_message(
                    clause,
                    &target_commit,
                    boot,
                    boot_commit,
                    at_subscribe,
                ),
            },
            urgency: DeliveryUrgency::Queue,
            muted: MutedDelivery::RideAlong,
            fact_key: None,
        },
    )
    .await?;
    Ok(RebuildEvaluation::Routed(action))
}

/// What subscribing did, so the caller can say it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebuildSubscribeOutcome {
    /// The current boot already contains the target; the wake fired now.
    FiredNow,
    /// The current boot contains it, but the router did not deliver (a closed
    /// thread, say); the row stays armed for the next containing boot.
    HeldByRouter,
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
            Self::HeldByRouter => format!(
                "Subscribed to a rebuild containing {target}. The current boot already contains it, but this home cannot take the wake now; it stays armed"
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
/// running would wait for a rebuild that may never carry anything newer.
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
        match evaluate_rebuild_subscription(orch, boot, containment, &subscription, clause, true)
            .await?
        {
            RebuildEvaluation::Routed(WakeRouteAction::Dropped) => {
                RebuildSubscribeOutcome::HeldByRouter
            }
            RebuildEvaluation::Routed(_) => RebuildSubscribeOutcome::FiredNow,
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
    pub delivered: usize,
    pub held: usize,
    pub waiting: usize,
    pub failures: usize,
}

/// Judge every armed rebuild subscription against `boot`, delivering those it
/// contains one at a time with `spacing` between deliveries.
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
    for subscription in pending_rebuild_subscriptions(orch).await? {
        let Some(clause) = RebuildClause::from_subscription(&subscription) else {
            log::warn!(
                "rebuild wake {} has a clause this runner cannot read; leaving it",
                subscription.id
            );
            pass.failures += 1;
            continue;
        };
        match evaluate_rebuild_subscription(orch, boot, containment, &subscription, &clause, false)
            .await
        {
            Ok(RebuildEvaluation::Routed(WakeRouteAction::Dropped)) => pass.held += 1,
            Ok(RebuildEvaluation::Routed(_)) => {
                pass.delivered += 1;
                if !spacing.is_zero() {
                    tokio::time::sleep(spacing).await;
                }
            }
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
    match evaluate_rebuild_wakes(orch, boot, &StoreContainment::new(orch), BOOT_DELIVERY_SPACING)
        .await
    {
        Ok(pass) => log::info!(
            "rebuild wakes for boot source_commit={}: {} delivered, {} held by their recipients, {} waiting, {} failed",
            boot.source_commit.as_deref().unwrap_or("unknown"),
            pass.delivered,
            pass.held,
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
