//! Declared regenerable paths after a base advance (cairn/7110).
//!
//! Two branches that both regenerate a committed build output (the URI
//! grammar's wasm pair) conflict on every replay even when their sources merge
//! cleanly, because no line merge can settle a derived file. A project declares
//! such outputs in `.cairn/config.yaml`'s `landing.regenerate`, read at the
//! commit being landed onto and never at the branch, so no branch chooses the
//! command Cairn runs on its behalf.
//!
//! # The replay
//!
//! When every conflict of a replay falls inside declared globs, preparation
//! recomputes with the engine taking the branch's bytes for those paths
//! ([`crate::jj::RegenerablePaths`]), inside the replay's own compute budget.
//! Before installing, it writes one `pending` row per rule under the store lock
//! ([`record_pending`]), so a branch never carries a taken side without its
//! marker. A conflict anywhere outside the globs stays a conflict exactly as
//! before. A check or run capture never installs: it asks for one owner
//! preparation ([`request_owner_preparation`]) and reports the regeneration as
//! pending.
//!
//! # The batch
//!
//! After the install and outside every lock, [`dispatch`] spawns the
//! regeneration; nothing in the replay awaits it (tenet 1442). It is one cell
//! at the installed head, admitted and counted like any lane, on a machine the
//! rule's selector matches; a selector nothing matches is refused by name. The
//! cell runs `regenerate`, then `check`. What they changed is sealed onto the
//! branch as a Cairn-authored commit, `regenerate <globs> after base advance`,
//! and only when it touches nothing outside the rule's globs. Like any check
//! output it is valid only for the head it ran on: a branch that moved
//! meanwhile supersedes it, and the row is run once more only when the stale
//! paths still hold the bytes that were taken.
//!
//! Trust: the commands are the base's, but they run the BRANCH's scripts in
//! the branch's checkout, exactly as a write-cadence lane does. The glob
//! restriction on the sealed delta is the containment, and nothing wider.
//!
//! # The hold
//!
//! A `pending` or `running` row holds the branch at the mutation boundary like
//! an in-flight run batch: every writer is refused ([`holding_except`]) save the
//! row's own sealed result, and a merge waits for the row to settle
//! ([`wait_for_settle`]) instead of being refused. A failed row releases the
//! hold and stays as a named fact: the owner is told once, and the merge reply,
//! the PR's checks block and the red lane say the paths are stale
//! ([`Regeneration::stale_line`]). Whether a stale-marked branch may merge is
//! the operator's approval policy, never this module's.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use cairn_common::executor_protocol::CellOwnerRef;
use cairn_db::turso::params;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::config::project_settings::{CheckResourceClass, LandingRegenerateRule};
use crate::execution::cache::InfraScopes;
use crate::execution::checks::{
    submit_planned_check_batch, CheckExecutionFailure, PlannedCheckBatchItem,
    PlannedCheckBatchRequest,
};
use crate::execution::wire::CheckResourceIdentityInput;
use crate::fleet::{CellPriority, MutationPolicy};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

pub(crate) const PENDING: &str = "pending";
pub(crate) const RUNNING: &str = "running";
pub(crate) const SUCCEEDED: &str = "succeeded";
pub(crate) const FAILED: &str = "failed";
pub(crate) const SUPERSEDED: &str = "superseded";
pub(crate) const CLEARED: &str = "cleared";

/// Each command's bound when the rule names none.
const DEFAULT_TIMEOUT_SECONDS: u32 = 600;
/// What the wait on a whole batch adds to its two commands' bounds: queueing,
/// placement, materialization and the seal.
const BATCH_SLACK: Duration = Duration::from_secs(300);
/// How much of a failing command's output the row and the notice keep.
const OUTPUT_TAIL_CHARS: usize = 4_000;
/// How many batches one row may run: its first, and one more after the branch
/// moved under it with the stale paths unchanged.
const MAX_DISPATCHES: i64 = 2;

/// One path a replay landed with the branch's bytes, and those bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StalePath {
    pub(crate) path: String,
    /// The branch's blob that was taken; `None` when the branch deleted it.
    pub(crate) blob: Option<String>,
}

/// Why a regeneration failed, as the owner and the merge are told.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RegenerationFailure {
    /// The command that failed, or the regenerate command when none ran.
    pub(crate) command: String,
    pub(crate) exit: Option<i32>,
    pub(crate) output_tail: String,
    /// Set when no exit code says what happened: nothing was placed, the
    /// batch was lost or outlived its bound, or its result could not land.
    pub(crate) reason: Option<String>,
}

impl RegenerationFailure {
    fn how(&self) -> String {
        match (self.exit, &self.reason) {
            (Some(exit), _) => format!("exit {exit}"),
            (None, Some(reason)) => reason.clone(),
            (None, None) => "no exit status".to_string(),
        }
    }
}

/// One rule's stale paths on one branch, and the batch regenerating them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Regeneration {
    pub(crate) id: i64,
    pub(crate) project_id: String,
    pub(crate) store_path: String,
    pub(crate) bookmark: String,
    /// The install that took these paths, shared by one row per rule.
    pub(crate) install_group: String,
    pub(crate) rule: LandingRegenerateRule,
    pub(crate) stale: Vec<StalePath>,
    pub(crate) installed_head: String,
    pub(crate) base_commit: String,
    pub(crate) state: String,
    pub(crate) request_id: Option<String>,
    pub(crate) failure: Option<RegenerationFailure>,
    pub(crate) commit: Option<String>,
    pub(crate) dispatches: i64,
    pub(crate) created_ms: i64,
    pub(crate) updated_ms: i64,
}

impl Regeneration {
    /// Whether this row holds its branch at the mutation boundary.
    pub(crate) fn holds(&self) -> bool {
        self.state == PENDING || self.state == RUNNING
    }

    /// The rule's globs, as the hold and the stale line name them.
    pub(crate) fn globs(&self) -> String {
        self.rule
            .paths
            .iter()
            .map(|glob| format!("`{glob}`"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The commit title the regeneration seals under.
    pub(crate) fn commit_message(&self) -> String {
        format!(
            "regenerate {} after base advance",
            self.rule.paths.join(", ")
        )
    }

    /// Where the hold settles: the batch once it has one.
    pub(crate) fn batch(&self) -> String {
        match &self.request_id {
            Some(request) => format!("batch `{request}` (regeneration #{})", self.id),
            None => format!("regeneration #{} once its batch is placed", self.id),
        }
    }

    /// What a writer held by this row is told.
    pub(crate) fn hold_text(&self) -> String {
        format!(
            "regenerating {} after base advance; settles at {}",
            self.globs(),
            self.batch()
        )
    }

    /// The named fact a failed row leaves wherever a merge is decided.
    pub(crate) fn stale_line(&self) -> Option<String> {
        let failure = self.failure.as_ref().filter(|_| self.state == FAILED)?;
        Some(format!(
            "{} is stale: regeneration failed (`{}`, {}); run `{}` on the branch",
            self.globs(),
            failure.command,
            failure.how(),
            self.rule.regenerate
        ))
    }

    /// Each command's bound.
    fn command_timeout(&self) -> Duration {
        Duration::from_secs(u64::from(
            self.rule.timeout.unwrap_or(DEFAULT_TIMEOUT_SECONDS),
        ))
    }

    /// How long a batch of this row may take before it is given up on, and
    /// how long a merge waits on it.
    pub(crate) fn settle_bound(&self) -> Duration {
        self.command_timeout() * 2 + BATCH_SLACK
    }
}

/// The job id a regeneration's cell is requested under. Its sealed result is
/// minted with it, which is how the row recognizes its own seal.
pub(crate) fn batch_job_id(id: i64) -> String {
    format!("regenerate:{id}")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn store_key(store: &Path) -> String {
    store.to_string_lossy().into_owned()
}

const COLUMNS: &str =
    "id, project_id, store_path, bookmark, rule_json, stale_json, installed_head, \
     base_commit, state, request_id, failure_json, commit_id, dispatches, created_ms, updated_ms, \
     install_group";

fn parse_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<Regeneration> {
    let decode = |column: &str, json: String| {
        crate::storage::DbError::internal(format!("branch_regenerations.{column}: {json}"))
    };
    let rule_json = row.text(4)?;
    let stale_json = row.text(5)?;
    let failure_json = row.opt_text(10)?;
    Ok(Regeneration {
        id: row.get::<i64>(0)?,
        project_id: row.text(1)?,
        store_path: row.text(2)?,
        bookmark: row.text(3)?,
        install_group: row.text(15)?,
        rule: serde_json::from_str(&rule_json).map_err(|_| decode("rule_json", rule_json))?,
        stale: serde_json::from_str(&stale_json).map_err(|_| decode("stale_json", stale_json))?,
        installed_head: row.text(6)?,
        base_commit: row.text(7)?,
        state: row.text(8)?,
        request_id: row.opt_text(9)?,
        failure: match failure_json {
            Some(json) => {
                Some(serde_json::from_str(&json).map_err(|_| decode("failure_json", json))?)
            }
            None => None,
        },
        commit: row.opt_text(11)?,
        dispatches: row.get::<i64>(12)?,
        created_ms: row.get::<i64>(13)?,
        updated_ms: row.get::<i64>(14)?,
    })
}

/// The rows this process recorded and is answerable for. A restart's recovery
/// ([`recover_after_restart`]) settles only rows outside it: one inside may be
/// between its record and its install right now.
/// Keyed by project as well as row: a row id is local to its database, and
/// the local database and every team replica number their rows alike.
type LiveRows = Mutex<HashSet<(String, i64)>>;

fn live_rows() -> &'static LiveRows {
    static LIVE: LazyLock<LiveRows> = LazyLock::new(Default::default);
    &LIVE
}

fn live(project_id: &str, ids: &[i64], held: bool) {
    let mut live = live_rows()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for id in ids {
        let key = (project_id.to_string(), *id);
        if held {
            live.insert(key);
        } else {
            live.remove(&key);
        }
    }
}

/// Forget every row this process recorded, as a restart does.
#[cfg(test)]
pub(crate) fn forget_live_rows() {
    live_rows().lock().unwrap().clear();
}

/// Mark the paths one install takes as awaiting their rules' regeneration,
/// one row per rule in one write, before the install that carries them; and
/// retire every earlier row still holding the branch, whose install is no
/// longer the branch's. Siblings of this install never supersede each other.
#[allow(clippy::too_many_arguments)] // One install's coordinates, named at the one writer.
pub(crate) async fn record_install(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    bookmark: &str,
    rules: &[(&LandingRegenerateRule, Vec<StalePath>)],
    installed_head: &str,
    base_commit: &str,
) -> Result<Vec<i64>, String> {
    let mut encoded = Vec::with_capacity(rules.len());
    for (rule, stale) in rules {
        encoded.push((
            serde_json::to_string(rule).map_err(|error| error.to_string())?,
            serde_json::to_string(stale).map_err(|error| error.to_string())?,
        ));
    }
    let owner = project_id.to_string();
    let (project_id, store, bookmark, head, base, group) = (
        project_id.to_string(),
        store_key(store),
        bookmark.to_string(),
        installed_head.to_string(),
        base_commit.to_string(),
        uuid::Uuid::new_v4().to_string(),
    );
    let now = now_ms();
    let ids = db
        .write(move |conn| {
            let (project_id, store, bookmark, head, base, group, encoded) = (
                project_id.clone(),
                store.clone(),
                bookmark.clone(),
                head.clone(),
                base.clone(),
                group.clone(),
                encoded.clone(),
            );
            Box::pin(async move {
                conn.execute(
                    "UPDATE branch_regenerations SET state = 'superseded', updated_ms = ?3
                      WHERE project_id = ?1 AND bookmark = ?2
                        AND state IN ('pending', 'running')",
                    params![project_id.as_str(), bookmark.as_str(), now],
                )
                .await?;
                let mut ids = Vec::with_capacity(encoded.len());
                for (rule, stale) in &encoded {
                    conn.execute(
                        "INSERT INTO branch_regenerations
                           (project_id, store_path, bookmark, install_group, rule_json,
                            stale_json, installed_head, base_commit, state, created_ms,
                            updated_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'pending', ?9, ?9)",
                        params![
                            project_id.as_str(),
                            store.as_str(),
                            bookmark.as_str(),
                            group.as_str(),
                            rule.as_str(),
                            stale.as_str(),
                            head.as_str(),
                            base.as_str(),
                            now
                        ],
                    )
                    .await?;
                    let mut rows = conn.query("SELECT last_insert_rowid()", ()).await?;
                    match rows.next().await? {
                        Some(row) => ids.push(row.get::<i64>(0)?),
                        None => {
                            return Err(crate::storage::DbError::internal(
                                "the inserted regeneration has no id",
                            ))
                        }
                    }
                }
                Ok(ids)
            })
        })
        .await
        .map_err(|error| format!("record a regeneration: {error}"))?;
    live(&owner, &ids, true);
    Ok(ids)
}

/// Remove rows whose install did not happen: nothing carries their paths.
pub(crate) async fn abandon(db: &LocalDb, project_id: &str, ids: &[i64]) {
    live(project_id, ids, false);
    for id in ids {
        if let Err(error) = db
            .execute(
                "DELETE FROM branch_regenerations WHERE id = ?1 AND state = 'pending'",
                params![*id],
            )
            .await
        {
            log::warn!("could not abandon regeneration #{id}: {error}");
        }
    }
}

/// The head the install actually left, when it differs from the candidate's.
pub(crate) async fn set_installed_head(db: &LocalDb, ids: &[i64], head: &str) {
    for id in ids {
        if let Err(error) = db
            .execute(
                "UPDATE branch_regenerations SET installed_head = ?2 WHERE id = ?1",
                params![*id, head],
            )
            .await
        {
            log::warn!("could not record regeneration #{id}'s installed head: {error}");
        }
    }
}

pub(crate) async fn load(db: &LocalDb, id: i64) -> Result<Option<Regeneration>, String> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM branch_regenerations WHERE id = ?1"),
        params![id],
        parse_row,
    )
    .await
    .map_err(|error| format!("read regeneration #{id}: {error}"))
}

/// The newest row of `branch` in any state, the one that says whether its
/// declared outputs are fresh.
pub(crate) async fn latest(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Option<Regeneration>, String> {
    db.query_opt(
        format!(
            "SELECT {COLUMNS} FROM branch_regenerations
              WHERE project_id = ?1 AND bookmark = ?2 ORDER BY id DESC LIMIT 1"
        ),
        params![project_id, branch],
        parse_row,
    )
    .await
    .map_err(|error| format!("read the regenerations of `{branch}`: {error}"))
}

/// The newest failed row of each rule on `branch` that no later row of that
/// rule settled: the stale facts a merge of `branch` is told.
pub(crate) async fn stale_failures(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<Regeneration>, String> {
    let rows = db
        .query_all(
            format!(
                "SELECT {COLUMNS} FROM branch_regenerations
                  WHERE project_id = ?1 AND bookmark = ?2 ORDER BY id DESC LIMIT 32"
            ),
            params![project_id, branch],
            parse_row,
        )
        .await
        .map_err(|error| format!("read the regenerations of `{branch}`: {error}"))?;
    let mut seen = HashSet::new();
    Ok(rows
        .into_iter()
        .filter(|row| seen.insert(row.rule.paths.clone()))
        .filter(|row| row.state == FAILED)
        .collect())
}

/// The row holding `branch` of `project_id`, newest first, except those of the
/// install whose regeneration's own sealed result is `exempt` (`(job,
/// request)` as the seal was minted): a regeneration's commit is the one write
/// its install's holds admit, its siblings' included, since they run one after
/// another and each needs the branch the others hold.
pub(crate) async fn holding_except(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    exempt: Option<(&str, &str)>,
) -> Result<Option<Regeneration>, String> {
    let rows = db
        .query_all(
            format!(
                "SELECT {COLUMNS} FROM branch_regenerations
                  WHERE project_id = ?1 AND bookmark = ?2 AND state IN ('pending', 'running')
                  ORDER BY id DESC"
            ),
            params![project_id, branch],
            parse_row,
        )
        .await
        .map_err(|error| format!("read the regenerations holding `{branch}`: {error}"))?;
    let exempt_group = exempt.and_then(|(job, request)| {
        rows.iter()
            .find(|row| job == batch_job_id(row.id) && row.request_id.as_deref() == Some(request))
            .map(|row| row.install_group.clone())
    });
    Ok(rows
        .into_iter()
        .find(|row| exempt_group.as_deref() != Some(row.install_group.as_str())))
}

pub(crate) async fn holding(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Option<Regeneration>, String> {
    holding_except(db, project_id, branch, None).await
}

async fn transition(
    db: &LocalDb,
    id: i64,
    sql: &'static str,
    values: impl cairn_db::turso::IntoParams + Send + 'static,
) -> Result<bool, String> {
    db.execute(sql, values)
        .await
        .map(|changed| changed > 0)
        .map_err(|error| format!("update regeneration #{id}: {error}"))
}

async fn mark_running(db: &LocalDb, id: i64) -> Result<bool, String> {
    transition(
        db,
        id,
        "UPDATE branch_regenerations
            SET state = 'running', request_id = NULL, dispatches = dispatches + 1, updated_ms = ?2
          WHERE id = ?1 AND state IN ('pending', 'running')",
        params![id, now_ms()],
    )
    .await
}

async fn record_request(db: &LocalDb, id: i64, request_id: &str) -> Result<bool, String> {
    transition(
        db,
        id,
        "UPDATE branch_regenerations SET request_id = ?2, updated_ms = ?3
          WHERE id = ?1 AND state = 'running'",
        params![id, request_id, now_ms()],
    )
    .await
}

async fn back_to_pending(db: &LocalDb, id: i64) -> Result<bool, String> {
    transition(
        db,
        id,
        "UPDATE branch_regenerations SET state = 'pending', updated_ms = ?2
          WHERE id = ?1 AND state = 'running'",
        params![id, now_ms()],
    )
    .await
}

async fn move_to_head(db: &LocalDb, id: i64, head: &str) -> Result<bool, String> {
    transition(
        db,
        id,
        "UPDATE branch_regenerations SET installed_head = ?2, updated_ms = ?3
          WHERE id = ?1 AND state IN ('pending', 'running')",
        params![id, head, now_ms()],
    )
    .await
}

/// A failed row whose paths a passing lane has since shown fresh.
async fn clear(db: &LocalDb, id: i64) -> Result<bool, String> {
    transition(
        db,
        id,
        "UPDATE branch_regenerations SET state = 'cleared', updated_ms = ?2
          WHERE id = ?1 AND state = 'failed'",
        params![id, now_ms()],
    )
    .await
}

/// Settle a holding row. `false` when it no longer held (a later install
/// superseded it), in which case nothing about it is announced.
async fn settle(
    db: &LocalDb,
    id: i64,
    state: &'static str,
    commit: Option<&str>,
    failure: Option<&RegenerationFailure>,
) -> Result<bool, String> {
    let failure = failure
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| error.to_string())?;
    transition(
        db,
        id,
        "UPDATE branch_regenerations
            SET state = ?2, commit_id = ?3, failure_json = ?4, updated_ms = ?5
          WHERE id = ?1 AND state IN ('pending', 'running')",
        params![id, state, commit, failure, now_ms()],
    )
    .await
}

/// Each branch's settle signal, by `(project, branch)`.
type SettleSignals = Mutex<HashMap<(String, String), Arc<tokio::sync::Notify>>>;

/// One signal per branch, raised whenever one of its rows settles.
fn settled_signal(project_id: &str, branch: &str) -> Arc<tokio::sync::Notify> {
    static SIGNALS: LazyLock<SettleSignals> = LazyLock::new(Default::default);
    SIGNALS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry((project_id.to_string(), branch.to_string()))
        .or_default()
        .clone()
}

/// Wait, holding nothing, until no row holds `branch`, and return the row
/// that held it when the wait began as it settled. `Ok(None)` when nothing
/// held it; an error when it still holds after `bound`.
pub(crate) async fn wait_for_settle(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    bound: Option<Duration>,
) -> Result<Option<Regeneration>, String> {
    let held = all_holding(db, project_id, branch).await?;
    let Some(first) = held.first().cloned() else {
        return Ok(None);
    };
    // Held rows run one after another, so the wait is their bounds' sum.
    let bound = bound.unwrap_or_else(|| serial_bound(&held));
    let deadline = Instant::now() + bound;
    let signal = settled_signal(project_id, branch);
    loop {
        let notified = signal.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        match holding(db, project_id, branch).await? {
            None => {
                return Ok(Some(load(db, first.id).await?.unwrap_or(first)));
            }
            Some(current) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(format!(
                        "`{branch}` is still {} after waiting {}s",
                        current.hold_text(),
                        bound.as_secs()
                    ));
                }
                // The row re-read is the authority; the signal only shortens
                // the wait, so a settlement in another process is still seen.
                let _ =
                    tokio::time::timeout((deadline - now).min(Duration::from_secs(5)), notified)
                        .await;
            }
        }
    }
}

// --- what the branch's surfaces say -----------------------------------------

/// Wait, holding nothing, for a regeneration holding `branch` to settle before
/// a merge lands it, and say so. `Ok(None)` when nothing held it. The merge
/// waits rather than reserving the branch: the regeneration's own commit
/// needs the reservation the landing would hold.
pub(crate) async fn wait_before_landing(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Option<String>, String> {
    let Some(settled) = wait_for_settle(db, project_id, branch, None)
        .await
        .map_err(refuse_to_land)?
    else {
        return Ok(None);
    };
    Ok(Some(settled_note(&settled)))
}

fn refuse_to_land(error: String) -> String {
    format!("Refusing to merge yet: {error}. Nothing changed; merge again.")
}

/// What a merge that waited on `row` says of it.
fn settled_note(row: &Regeneration) -> String {
    format!(
        "Waited for regeneration of {} after base advance ({}): {}.",
        row.globs(),
        row.batch(),
        match row.state.as_str() {
            SUCCEEDED => match &row.commit {
                Some(commit) => format!("regenerated as `{commit}`"),
                None => "they were already fresh".to_string(),
            },
            FAILED => "it failed".to_string(),
            other => other.to_string(),
        }
    )
}

/// The rows of `branch` recorded after row `after`, oldest first.
async fn rows_after(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    after: i64,
) -> Result<Vec<Regeneration>, String> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM branch_regenerations
              WHERE project_id = ?1 AND bookmark = ?2 AND id > ?3 ORDER BY id"
        ),
        params![project_id, branch, after],
        parse_row,
    )
    .await
    .map_err(|error| format!("read the regenerations of `{branch}`: {error}"))
}

/// Every row holding `branch`, oldest first.
async fn all_holding(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<Regeneration>, String> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM branch_regenerations
              WHERE project_id = ?1 AND bookmark = ?2 AND state IN ('pending', 'running')
              ORDER BY id"
        ),
        params![project_id, branch],
        parse_row,
    )
    .await
    .map_err(|error| format!("read the regenerations holding `{branch}`: {error}"))
}

/// How long rows that run one after another may take together: the sum of
/// their own bounds, never the largest one alone.
fn serial_bound(rows: &[Regeneration]) -> Duration {
    rows.iter().map(Regeneration::settle_bound).sum()
}

/// Wait, holding nothing, until none of `rows` holds its branch, however
/// early they settled; each one as it settled.
async fn wait_for_rows(db: &LocalDb, rows: &[Regeneration]) -> Result<Vec<Regeneration>, String> {
    wait_for_rows_within(db, rows, serial_bound(rows)).await
}

async fn wait_for_rows_within(
    db: &LocalDb,
    rows: &[Regeneration],
    bound: Duration,
) -> Result<Vec<Regeneration>, String> {
    let Some(first) = rows.first() else {
        return Ok(Vec::new());
    };
    let deadline = Instant::now() + bound;
    let signal = settled_signal(&first.project_id, &first.bookmark);
    loop {
        let notified = signal.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut now_rows = Vec::with_capacity(rows.len());
        for row in rows {
            now_rows.push(load(db, row.id).await?.unwrap_or_else(|| row.clone()));
        }
        let Some(holding) = now_rows.iter().find(|row| row.holds()) else {
            return Ok(now_rows);
        };
        let now = Instant::now();
        if now >= deadline {
            return Err(format!(
                "`{}` is still {} after waiting {}s",
                holding.bookmark,
                holding.hold_text(),
                bound.as_secs()
            ));
        }
        let _ = tokio::time::timeout((deadline - now).min(Duration::from_secs(5)), notified).await;
    }
}

/// Run a landing `attempt` of `branch`, first waiting, holding nothing, on a
/// regeneration that holds it, and once more on the rows the attempt itself
/// recorded: a landing whose own preparation takes regenerable paths installs
/// them, dispatches their regeneration and refuses, and lands after they
/// settle. The retry is keyed on those rows, so one that settles before this
/// looks is still waited past. Returns the attempt's result and what was
/// waited for; an error only when a wait outlived its bound.
pub(crate) async fn land_after_regenerations<F, Fut>(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    mut attempt: F,
) -> Result<(Result<String, String>, Vec<String>), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<String, String>>,
{
    let mut notes: Vec<String> = wait_before_landing(db, project_id, branch)
        .await?
        .into_iter()
        .collect();
    let mut retries = 1;
    loop {
        let before = latest(db, project_id, branch)
            .await?
            .map_or(0, |row| row.id);
        let result = attempt().await;
        if result.is_err() && retries > 0 {
            let recorded = rows_after(db, project_id, branch, before).await?;
            if !recorded.is_empty() {
                retries -= 1;
                let settled = wait_for_rows(db, &recorded).await.map_err(refuse_to_land)?;
                notes.extend(settled.iter().map(settled_note));
                continue;
            }
        }
        return Ok((result, notes));
    }
}

/// Whether the lanes that passed at the branch's head clear `row`: only the
/// lane its rule names as the proof of its outputs ([`LandingRegenerateRule::lane`])
/// does, and it proves the rule's whole output. A lane whose impact merely
/// includes the paths reads them as inputs and proves nothing about them.
fn cleared_by(row: &Regeneration, passing: &[String]) -> bool {
    row.rule
        .lane
        .as_ref()
        .is_some_and(|lane| passing.contains(lane))
}

/// The lanes that passed at `head` of `branch`.
async fn passing_lanes(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    head: &str,
) -> Vec<String> {
    let Ok(jobs) = super::load_on_branch_jobs(db, project_id, branch).await else {
        return Vec::new();
    };
    let Some(owner) = super::branch_owners(&jobs)
        .first()
        .map(|job| job.id.clone())
    else {
        return Vec::new();
    };
    let Some(statuses) =
        crate::execution::checks_status::node_check_statuses_at_head(orch, &owner, head).await
    else {
        return Vec::new();
    };
    statuses
        .into_iter()
        .filter(|lane| lane.state == crate::execution::checks_status::NodeCheckState::Passed)
        .map(|lane| lane.name)
        .collect()
}

/// `branch`'s head and the blob each path holds there.
async fn read_head(
    orch: &Orchestrator,
    store: &Path,
    branch: &str,
) -> Result<(String, HashMap<String, String>), String> {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (store, name) = (store.to_path_buf(), branch.to_string());
    tokio::task::spawn_blocking(move || {
        let head = crate::jj::bookmark_commit(&jj, &store, &name)
            .ok_or_else(|| format!("`{name}` has no bookmark"))?;
        let entries = crate::jj::tree_entries(&jj, &store, &head)?;
        Ok((head, entries.into_iter().collect()))
    })
    .await
    .map_err(|error| error.to_string())?
}

/// Whether `blobs` still holds exactly what the replay took for `row`.
fn taken_bytes_stand(row: &Regeneration, blobs: &HashMap<String, String>) -> bool {
    row.stale
        .iter()
        .all(|stale| blobs.get(&stale.path).map(String::as_str) == stale.blob.as_deref())
}

/// The failed regenerations of `branch` that still stand, each with its stale
/// line. A row whose rule's proving lane passed at the branch's head is
/// cleared first: that lane is the gate, so its green is the evidence the
/// outputs are fresh, whether the owner regenerated them by hand or they
/// never needed it. A branch that changed the paths without it says so.
async fn standing_failures(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<(Regeneration, String)>, String> {
    let rows = stale_failures(db, project_id, branch).await?;
    let Some(first) = rows.first() else {
        return Ok(Vec::new());
    };
    let head = read_head(orch, Path::new(&first.store_path), branch)
        .await
        .ok();
    let passing = match &head {
        Some((head, _)) => passing_lanes(orch, db, project_id, branch, head).await,
        None => Vec::new(),
    };
    let mut standing = Vec::new();
    for row in rows {
        if cleared_by(&row, &passing) {
            if let Err(error) = clear(db, row.id).await {
                log::warn!("{error}");
            }
            continue;
        }
        let Some(line) = row.stale_line() else {
            continue;
        };
        let changed = head
            .as_ref()
            .is_some_and(|(_, blobs)| !taken_bytes_stand(&row, blobs));
        let line = if changed {
            format!(
                "{line}. The branch has changed those paths since, and the lane proving them \
                 has not passed at its head yet"
            )
        } else {
            line
        };
        standing.push((row, line));
    }
    Ok(standing)
}

/// The stale lines `branch` carries now ([`standing_failures`]).
pub(crate) async fn stale_lines(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Vec<String> {
    match standing_failures(orch, db, project_id, branch).await {
        Ok(standing) => standing.into_iter().map(|(_, line)| line).collect(),
        Err(error) => vec![format!(
            "whether `{branch}` carries stale declared outputs is unknown: {error}"
        )],
    }
}

/// What the PR shows of `branch`'s declared outputs: the hold while one is
/// regenerating, else the stale lines. `None` when there is nothing to say.
pub(crate) async fn pull_request_note(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Option<String> {
    if let Ok(Some(row)) = holding(db, project_id, branch).await {
        return Some(format!(
            "⏳ `{branch}` is {}; a merge waits for it to settle.",
            row.hold_text()
        ));
    }
    let lines = stale_lines(orch, db, project_id, branch).await;
    (!lines.is_empty()).then(|| {
        lines
            .iter()
            .map(|line| format!("⚠️ {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    })
}

/// The lines a failed lane of `job_id` carries: each stale line of the job's
/// branch, under every failed lane whose declared impact covers a stale path.
pub(crate) async fn failed_lane_lines(
    orch: &Orchestrator,
    job_id: &str,
    failed_lanes: &[String],
) -> Vec<String> {
    if failed_lanes.is_empty() {
        return Vec::new();
    }
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await else {
        return Vec::new();
    };
    let found = db
        .query_opt(
            "SELECT j.project_id, j.branch, p.repo_path FROM jobs j
               JOIN projects p ON p.id = j.project_id WHERE j.id = ?1",
            params![job_id],
            |row| Ok((row.text(0)?, row.opt_text(1)?, row.text(2)?)),
        )
        .await;
    let Ok(Some((project_id, Some(branch), repo))) = found else {
        return Vec::new();
    };
    let standing = match standing_failures(orch, &db, &project_id, &branch).await {
        Ok(standing) if !standing.is_empty() => standing,
        _ => return Vec::new(),
    };
    let Some(contract) = crate::config::project_settings::load_checks_contract(Path::new(&repo))
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for lane in failed_lanes {
        let Some(impact) = contract
            .checks
            .get(lane)
            .and_then(|check| check.impact.as_ref())
        else {
            continue;
        };
        for (row, line) in &standing {
            if row
                .stale
                .iter()
                .any(|stale| crate::orchestrator::landing::path_matches(impact, &stale.path))
            {
                out.push(format!("`{lane}` is red, and {line}"));
            }
        }
    }
    out
}

/// The rebase page's account of `row`, the branch's newest regeneration.
pub(crate) fn page_section(row: &Regeneration) -> String {
    let state = match row.state.as_str() {
        PENDING | RUNNING => format!(
            "{} carry this branch's own bytes after the base advance to `{}`, and are {}. \
             Nothing moves the branch until it settles, and a merge waits for it.",
            row.globs(),
            row.base_commit,
            row.hold_text()
        ),
        SUCCEEDED => match &row.commit {
            Some(commit) => format!(
                "{} were regenerated after the base advance to `{}`, as `{commit}`.",
                row.globs(),
                row.base_commit
            ),
            None => format!(
                "{} were already fresh after the base advance to `{}`: nothing was committed.",
                row.globs(),
                row.base_commit
            ),
        },
        FAILED => format!(
            "⚠️ {}. Run it again here with \
             `write({{changes:[{{target:\"cairn:~/rebase\",mode:\"patch\",payload:{{action:\"regenerate\"}}}}]}})`.{}",
            row.stale_line().unwrap_or_default(),
            row.failure
                .as_ref()
                .filter(|failure| !failure.output_tail.is_empty())
                .map(|failure| format!("\n\n```\n{}\n```", failure.output_tail))
                .unwrap_or_default()
        ),
        CLEARED => format!(
            "Regenerating {} after the base advance to `{}` failed, and a lane covering them \
             has since passed at the branch's head: they are fresh.",
            row.globs(),
            row.base_commit
        ),
        _ => format!(
            "The regeneration of {} after the base advance to `{}` was superseded: the branch \
             changed those paths, a later replay took them again, or its install never \
             happened.",
            row.globs(),
            row.base_commit
        ),
    };
    format!(
        "\n## Regenerable outputs\n\n{state} Taken from the branch: {}.\n\n",
        row.stale
            .iter()
            .map(|stale| format!("`{}`", stale.path))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Run `branch`'s newest regeneration again at its current head, at its
/// owner's request: after a failure, or to confirm a hand regeneration.
pub(crate) async fn redispatch(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    project_id: &str,
    store: &Path,
    branch: &str,
) -> Result<String, String> {
    if let Some(row) = holding(&db, project_id, branch).await? {
        return Err(format!("`{branch}` is already {}", row.hold_text()));
    }
    let Some(last) = latest(&db, project_id, branch).await? else {
        return Err(format!(
            "`{branch}` has no declared regeneration to run again: no replay of it has taken a \
             regenerable path"
        ));
    };
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (path, name) = (store.to_path_buf(), branch.to_string());
    let head = tokio::task::spawn_blocking(move || crate::jj::bookmark_commit(&jj, &path, &name))
        .await
        .ok()
        .flatten()
        .ok_or_else(|| format!("`{branch}` has no head in the project store"))?;
    let ids = record_install(
        &db,
        project_id,
        store,
        branch,
        &[(&last.rule, last.stale.clone())],
        &head,
        &last.base_commit,
    )
    .await?;
    let id = ids[0];
    dispatch(orch, db, project_id, &ids);
    Ok(format!(
        "Regenerating {} at `{head}` (regeneration #{id}). Nothing moves `{branch}` until it \
         settles; cairn:~/rebase shows how it ends.",
        last.globs()
    ))
}

// --- dispatch ---------------------------------------------------------------

/// What a regeneration batch is asked to do.
pub(crate) struct RegenerationJob {
    pub(crate) row: Regeneration,
    /// The project's repository, which a sealed delta publishes into.
    pub(crate) repository: PathBuf,
    pub(crate) store: PathBuf,
    pub(crate) project_key: String,
    /// The branch's owner, whose work the batch is counted as.
    pub(crate) owner_job_id: Option<String>,
}

/// How one command of the batch ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommandEnding {
    pub(crate) exit: Option<i32>,
    pub(crate) timed_out: bool,
    pub(crate) output: String,
}

impl CommandEnding {
    fn passed(&self) -> bool {
        !self.timed_out && self.exit == Some(0)
    }
}

/// What a regeneration batch came back with.
pub(crate) enum BatchOutcome {
    /// The cell ran. Each command's ending, or why it has none; and what the
    /// commands changed, when anything.
    Ran {
        request_id: Option<String>,
        regenerate: Result<CommandEnding, String>,
        check: Result<CommandEnding, String>,
        delta: Option<Box<dyn RegeneratedDelta>>,
    },
    /// Nothing ran, for this reason: no machine matches, or the batch could
    /// not be submitted.
    NotRun { reason: String },
}

/// A regeneration's changes, not yet on the branch.
pub(crate) trait RegeneratedDelta: Send + Sync {
    /// Seal onto `branch` as a Cairn-authored commit titled `message`, only if
    /// every changed path is one `admits` accepts and only onto the head the
    /// batch ran at. `Ok(Some(commit))` landed (or found already landed) at
    /// `commit`; `Ok(None)` was superseded by a branch that moved.
    fn seal<'a>(
        self: Box<Self>,
        orch: &'a Orchestrator,
        branch: &'a str,
        message: &'a str,
        admits: &'a (dyn Fn(&str) -> bool + Send + Sync),
    ) -> BoxFuture<'a, Result<Option<String>, String>>;
}

/// How a regeneration batch runs. Production places it through the fleet
/// ([`FleetRunner`]); a test runs it in a local checkout.
pub(crate) trait RegenerationRunner: Send + Sync {
    fn run<'a>(
        &'a self,
        orch: &'a Orchestrator,
        job: &'a RegenerationJob,
    ) -> BoxFuture<'a, BatchOutcome>;
}

#[cfg(test)]
fn test_runners() -> &'static Mutex<HashMap<String, Arc<dyn RegenerationRunner>>> {
    static RUNNERS: LazyLock<Mutex<HashMap<String, Arc<dyn RegenerationRunner>>>> =
        LazyLock::new(Default::default);
    &RUNNERS
}

/// Run regenerations of `store` with `runner` instead of the fleet.
#[cfg(test)]
pub(crate) fn install_test_runner(store: &Path, runner: Arc<dyn RegenerationRunner>) {
    test_runners()
        .lock()
        .unwrap()
        .insert(store_key(store), runner);
}

fn runner_for(store: &str) -> Arc<dyn RegenerationRunner> {
    #[cfg(test)]
    if let Some(runner) = test_runners().lock().unwrap().get(store) {
        return runner.clone();
    }
    let _ = store;
    Arc::new(FleetRunner)
}

/// Start the regeneration of one install's rows, one after another in one
/// task: each seals onto the head its predecessor left, so siblings never
/// supersede each other. Returns at once: the replay that installed the rows
/// never waits on their batches.
pub(crate) fn dispatch(orch: &Orchestrator, db: Arc<LocalDb>, project_id: &str, ids: &[i64]) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        log::warn!(
            "regenerations {ids:?} were installed outside a runtime; the next start recovers them"
        );
        live(project_id, ids, false);
        return;
    };
    let (orch, ids, project_id) = (orch.clone(), ids.to_vec(), project_id.to_string());
    runtime.spawn(async move {
        for id in &ids {
            run_row(&orch, &db, *id).await;
        }
        live(&project_id, &ids, false);
    });
}

/// One orphaned install: its project, its group and its rows in order.
type Orphan = (String, String, Vec<i64>);

/// The installs among `rows` that no task of this process answers for,
/// `owned` being the `(project, row)` pairs it does.
fn orphan_installs(rows: Vec<Regeneration>, owned: &HashSet<(String, i64)>) -> Vec<Orphan> {
    let mut groups: Vec<Orphan> = Vec::new();
    for row in rows
        .into_iter()
        .filter(|row| !owned.contains(&(row.project_id.clone(), row.id)))
    {
        match groups
            .iter_mut()
            .find(|(project, group, _)| *project == row.project_id && *group == row.install_group)
        {
            Some((_, _, ids)) => ids.push(row.id),
            None => groups.push((row.project_id, row.install_group, vec![row.id])),
        }
    }
    groups
}

/// Settle or resume what a previous process left holding a branch. Its tasks
/// died with it, so a row it recorded and never settled would otherwise hold
/// its branch forever. Each such install's rows run again from `pending`:
/// one whose install never happened, or whose paths someone has since
/// changed, is superseded before anything runs, and the rest regenerate at
/// the branch's head. A batch of the old process that is still running
/// somewhere is not waited for: its result has nobody left to deliver it to.
/// Rows this process recorded are left to it.
pub(crate) async fn recover_after_restart(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        let rows = match db
            .query_all(
                format!(
                    "SELECT {COLUMNS} FROM branch_regenerations
                      WHERE state IN ('pending', 'running') ORDER BY id"
                ),
                (),
                parse_row,
            )
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                log::warn!("regenerations left by a restart could not be read: {error}");
                continue;
            }
        };
        let owned = live_rows()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        for (project_id, group, ids) in orphan_installs(rows, &owned) {
            for id in &ids {
                if let Err(error) = back_to_pending(&db, *id).await {
                    log::warn!("{error}");
                }
            }
            log::info!(
                "recovering regenerations {ids:?} of project {project_id} (install {group}) left \
                 by a restart"
            );
            live(&project_id, &ids, true);
            dispatch(orch, db.clone(), &project_id, &ids);
        }
    }
}

async fn run_row(orch: &Orchestrator, db: &LocalDb, id: i64) {
    loop {
        let row = match load(db, id).await {
            Ok(Some(row)) if row.holds() => row,
            Ok(_) => return,
            Err(error) => {
                log::warn!("regeneration #{id} could not be read to dispatch: {error}");
                return;
            }
        };
        match run_once(orch, db, row).await {
            Next::Done => return,
            Next::Again => continue,
        }
    }
}

enum Next {
    Done,
    Again,
}

/// Bring `row` to its branch's head before it runs. The head it was recorded
/// at moves only by its siblings' commits (the hold admits nothing else), or
/// it may never have been installed (a restart between record and install).
/// A row whose install the branch does not contain, or whose taken bytes the
/// head no longer holds, is superseded; otherwise it runs at the head.
async fn catch_up(
    orch: &Orchestrator,
    db: &LocalDb,
    job: &RegenerationJob,
    row: Regeneration,
) -> Option<Regeneration> {
    let (head, blobs) = match read_head(orch, &job.store, &row.bookmark).await {
        Ok(read) => read,
        Err(error) => {
            fail(
                orch,
                db,
                &row,
                not_run(&row, format!("the branch is unreadable: {error}")),
            )
            .await;
            return None;
        }
    };
    if head == row.installed_head {
        return Some(row);
    }
    let contains_install = crate::env::git()
        .args(["merge-base", "--is-ancestor", &row.installed_head, &head])
        .current_dir(&job.repository)
        .status()
        .map(|status| status.success())
        // An unreadable ancestry is not evidence the install never happened.
        .unwrap_or(true);
    if !contains_install || !taken_bytes_stand(&row, &blobs) {
        log::info!(
            "regeneration #{} of `{}` is superseded at {head}: {}",
            row.id,
            row.bookmark,
            if contains_install {
                "the branch changed its stale paths"
            } else {
                "the branch does not contain its install"
            }
        );
        if let Ok(true) = settle(db, row.id, SUPERSEDED, None, None).await {
            released(orch, &row);
        }
        return None;
    }
    match move_to_head(db, row.id, &head).await {
        Ok(true) => Some(Regeneration {
            installed_head: head,
            ..row
        }),
        Ok(false) => None,
        Err(error) => {
            log::warn!("{error}");
            None
        }
    }
}

async fn run_once(orch: &Orchestrator, db: &LocalDb, row: Regeneration) -> Next {
    let id = row.id;
    let mut job = match job_for(db, &row).await {
        Ok(job) => job,
        Err(reason) => {
            fail(orch, db, &row, not_run(&row, reason)).await;
            return Next::Done;
        }
    };
    let Some(row) = catch_up(orch, db, &job, row).await else {
        return Next::Done;
    };
    job.row = row.clone();
    match mark_running(db, id).await {
        Ok(true) => {}
        Ok(false) => return Next::Done,
        Err(error) => {
            log::warn!("{error}");
            return Next::Done;
        }
    }
    let runner = runner_for(&row.store_path);
    let bound = row.settle_bound();
    let outcome = match tokio::time::timeout(bound, runner.run(orch, &job)).await {
        Ok(outcome) => outcome,
        Err(_) => {
            let reason = format!("did not settle within {}s", bound.as_secs());
            fail(orch, db, &row, not_run(&row, reason)).await;
            return Next::Done;
        }
    };
    let (request_id, regenerate, check, delta) = match outcome {
        BatchOutcome::NotRun { reason } => {
            fail(orch, db, &row, not_run(&row, reason)).await;
            return Next::Done;
        }
        BatchOutcome::Ran {
            request_id,
            regenerate,
            check,
            delta,
        } => (request_id, regenerate, check, delta),
    };
    let mut row = row;
    if let Some(request_id) = &request_id {
        match record_request(db, id, request_id).await {
            Ok(true) => row.request_id = Some(request_id.clone()),
            Ok(false) => return Next::Done,
            Err(error) => log::warn!("{error}"),
        }
    }
    for (command, ending) in [
        (&row.rule.regenerate, &regenerate),
        (&row.rule.check, &check),
    ] {
        let failure = match ending {
            Ok(ending) if ending.passed() => continue,
            Ok(ending) => RegenerationFailure {
                command: command.clone(),
                exit: ending.exit,
                output_tail: tail(&ending.output),
                reason: ending.timed_out.then(|| "timed out".to_string()),
            },
            Err(reason) => RegenerationFailure {
                command: command.clone(),
                exit: None,
                output_tail: String::new(),
                reason: Some(reason.clone()),
            },
        };
        fail(orch, db, &row, failure).await;
        return Next::Done;
    }
    let Some(delta) = delta else {
        // The branch's bytes were already what its sources derive.
        succeed(orch, db, &row, None).await;
        return Next::Done;
    };
    let rule = row.rule.paths.clone();
    let admits = move |path: &str| crate::orchestrator::landing::path_matches(&rule, path);
    let sealed = delta
        .seal(orch, &row.bookmark, &row.commit_message(), &admits)
        .await;
    match sealed {
        Ok(Some(commit)) if commit == row.installed_head => {
            succeed(orch, db, &row, None).await;
            Next::Done
        }
        Ok(Some(commit)) => {
            succeed(orch, db, &row, Some(&commit)).await;
            Next::Done
        }
        Ok(None) => superseded(orch, db, &row).await,
        Err(reason) => {
            let failure = RegenerationFailure {
                command: row.rule.regenerate.clone(),
                exit: None,
                output_tail: String::new(),
                reason: Some(reason),
            };
            fail(orch, db, &row, failure).await;
            Next::Done
        }
    }
}

/// The batch's result arrived after the branch moved. Run once more: the
/// next run's [`catch_up`] moves it to the new head when the stale paths
/// still hold exactly the bytes that were taken, and supersedes it when
/// someone changed them.
async fn superseded(orch: &Orchestrator, db: &LocalDb, row: &Regeneration) -> Next {
    if row.dispatches >= MAX_DISPATCHES - 1 {
        let failure = RegenerationFailure {
            command: row.rule.regenerate.clone(),
            exit: None,
            output_tail: String::new(),
            reason: Some("superseded twice".to_string()),
        };
        fail(orch, db, row, failure).await;
        return Next::Done;
    }
    match back_to_pending(db, row.id).await {
        Ok(true) => Next::Again,
        Ok(false) => Next::Done,
        Err(error) => {
            log::warn!("{error}");
            Next::Done
        }
    }
}

fn not_run(row: &Regeneration, reason: String) -> RegenerationFailure {
    RegenerationFailure {
        command: row.rule.regenerate.clone(),
        exit: None,
        output_tail: String::new(),
        reason: Some(reason),
    }
}

fn tail(output: &str) -> String {
    let chars = output.chars().count();
    let tail: String = output
        .chars()
        .skip(chars.saturating_sub(OUTPUT_TAIL_CHARS))
        .collect();
    tail.trim_end().to_string()
}

async fn succeed(orch: &Orchestrator, db: &LocalDb, row: &Regeneration, commit: Option<&str>) {
    match settle(db, row.id, SUCCEEDED, commit, None).await {
        Ok(true) => {
            log::info!(
                "regeneration #{} of `{}` succeeded{}",
                row.id,
                row.bookmark,
                commit
                    .map(|commit| format!(" as {commit}"))
                    .unwrap_or_else(|| ": the paths were already fresh".into())
            );
            released(orch, row);
        }
        Ok(false) => {}
        Err(error) => log::warn!("{error}"),
    }
}

async fn fail(orch: &Orchestrator, db: &LocalDb, row: &Regeneration, failure: RegenerationFailure) {
    match settle(db, row.id, FAILED, None, Some(&failure)).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            log::warn!("{error}");
            return;
        }
    }
    let mut failed = row.clone();
    failed.state = FAILED.to_string();
    failed.failure = Some(failure.clone());
    let line = failed.stale_line().unwrap_or_default();
    log::warn!("regeneration #{} of `{}`: {line}", row.id, row.bookmark);
    let output = if failure.output_tail.is_empty() {
        String::new()
    } else {
        format!("\n\n```\n{}\n```", failure.output_tail)
    };
    let note = format!(
        "[Branch update] After a base advance your branch `{}` carries {} with its own bytes, and \
         regenerating them failed: {line}.{output}",
        row.bookmark,
        failed.globs()
    );
    super::notify_on_branch_jobs(
        orch,
        db,
        &row.project_id,
        &row.bookmark,
        &note,
        DeliveryUrgency::Steer,
        &format!("regenerate:{}", row.id),
    )
    .await;
    released(orch, row);
}

/// A row settled: wake whoever waits on the branch and serve what its hold
/// deferred.
fn released(orch: &Orchestrator, row: &Regeneration) {
    settled_signal(&row.project_id, &row.bookmark).notify_waiters();
    let (orch, project_id, branch) = (orch.clone(), row.project_id.clone(), row.bookmark.clone());
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    runtime.spawn(async move {
        let Ok(db) = crate::execution::routing::owning_db_for_project(&orch.db, &project_id).await
        else {
            return;
        };
        if let Ok(jobs) = super::load_on_branch_jobs(&db, &project_id, &branch).await {
            if let Some(owner) = super::branch_owners(&jobs).first() {
                super::on_branch_hold_released(&orch, &owner.id);
            }
        }
    });
}

async fn job_for(db: &LocalDb, row: &Regeneration) -> Result<RegenerationJob, String> {
    let (project_key, repository) = db
        .query_opt(
            "SELECT key, repo_path FROM projects WHERE id = ?1",
            params![row.project_id.as_str()],
            |found| Ok((found.text(0)?, found.text(1)?)),
        )
        .await
        .map_err(|error| format!("the project is unreadable: {error}"))?
        .ok_or_else(|| format!("project {} no longer exists", row.project_id))?;
    let owner_job_id = super::load_on_branch_jobs(db, &row.project_id, &row.bookmark)
        .await
        .ok()
        .and_then(|jobs| {
            super::branch_owners(&jobs)
                .first()
                .map(|job| job.id.clone())
        });
    Ok(RegenerationJob {
        row: row.clone(),
        repository: PathBuf::from(repository),
        store: PathBuf::from(&row.store_path),
        project_key,
        owner_job_id,
    })
}

/// Asks for a branch's owner preparation, once per base coordinate, on behalf
/// of a capture that met a conflict only regeneration settles: captures never
/// install, so the owner's replay does, and dispatches the batch.
pub(crate) fn request_owner_preparation(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
    base_commit: &str,
) {
    static REQUESTED: LazyLock<Mutex<HashSet<(String, String, String)>>> =
        LazyLock::new(Default::default);
    let key = (
        project_id.to_string(),
        branch.to_string(),
        base_commit.to_string(),
    );
    if !REQUESTED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(key.clone())
    {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let orch = orch.clone();
    runtime.spawn(async move {
        use super::preparation::{prepare_branch, PreparationPurpose, PreparationRequest};
        let (project_id, branch, _) = &key;
        let request = PreparationRequest::new(
            project_id.clone(),
            branch.clone(),
            None,
            PreparationPurpose::OwnerCommit,
        );
        if let Err(refusal) = prepare_branch(&orch, request).await {
            log::info!("the owner preparation a capture asked for on `{branch}` did not install: {refusal}");
            // A later capture at this base may ask again.
            REQUESTED
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&key);
        }
    });
}

// --- the fleet ---------------------------------------------------------------

/// Places the regeneration as one two-item cell whose changes come back.
struct FleetRunner;

impl RegenerationRunner for FleetRunner {
    fn run<'a>(
        &'a self,
        orch: &'a Orchestrator,
        job: &'a RegenerationJob,
    ) -> BoxFuture<'a, BatchOutcome> {
        Box::pin(run_in_fleet(orch, job))
    }
}

async fn run_in_fleet(orch: &Orchestrator, job: &RegenerationJob) -> BatchOutcome {
    let row = &job.row;
    let requester = batch_job_id(row.id);
    let head = row.installed_head.clone();
    let timeout_ms = u64::try_from(row.command_timeout().as_millis()).unwrap_or(u64::MAX);
    let item = |index: usize, name: &str, command: &str| PlannedCheckBatchItem {
        recording_plan: None,
        index,
        name: name.to_string(),
        input_hash: format!("regenerate:{}:{head}:{index}", row.id),
        resource_identity: CheckResourceIdentityInput::Checkpoint {
            name: name.to_string(),
            command: command.to_string(),
        },
        command: command.to_string(),
        stream_id: format!("regenerate:{}:{index}", row.id),
        env: Vec::new(),
        verdict_environment_names: Vec::new(),
        timeout_ms,
        executor: row.rule.executor.clone(),
        verdict_platforms: Vec::new(),
        resource_class: CheckResourceClass::Shared,
        infra_scopes: InfraScopes::head_only(&requester, &head),
        avoid_executor_ids: Vec::new(),
    };
    let submitted = submit_planned_check_batch(
        orch,
        PlannedCheckBatchRequest {
            project_id: row.project_id.clone(),
            repository: job.repository.to_string_lossy().into_owned(),
            store_dir: job.store.clone(),
            sealed_commit: head.clone(),
            requesting_job_id: requester.clone(),
            owner: CellOwnerRef {
                project_id: row.project_id.clone(),
                project_key: Some(job.project_key.clone()),
                issue_number: None,
                job_id: job.owner_job_id.clone(),
                execution_seq: None,
                node_kind: Some("regenerate".to_string()),
            },
            affinity_key: None,
            priority: CellPriority::WriteCheck,
            env: Vec::new(),
            items: vec![
                item(0, "regenerate", &row.rule.regenerate),
                item(1, "regenerate check", &row.rule.check),
            ],
            cell_groups: Default::default(),
            run_context: None,
            mutation_policy: MutationPolicy::AllowDelta,
            status_board: None,
            on_group_complete: None,
            purpose: None,
            publication: None,
        },
    )
    .await;
    let mut outcome = match submitted {
        Ok(outcome) => outcome,
        Err(error) => {
            return BatchOutcome::NotRun {
                reason: format!("the batch could not be submitted: {error}"),
            }
        }
    };
    let mut ending = |index: usize| match outcome.results.remove(&index) {
        Some(Ok(ran)) => Ok(CommandEnding {
            exit: ran.exit_code,
            timed_out: ran.timed_out,
            output: ran.output,
        }),
        Some(Err(failure)) => Err(describe_failure(&failure)),
        None => Err("the batch returned no result for it".to_string()),
    };
    let (regenerate, check) = (ending(0), ending(1));
    if let (Err(reason), Err(_)) = (&regenerate, &check) {
        if outcome.request.is_none() {
            return BatchOutcome::NotRun {
                reason: reason.clone(),
            };
        }
    }
    let request_id = outcome
        .request
        .as_ref()
        .map(|request| request.request_id.clone());
    let delta = match (outcome.delta, outcome.request, outcome.store_dir) {
        (Some(delta), Some(request), Some(store_dir)) => Some(Box::new(FleetDelta {
            store_dir,
            request,
            delta,
        })
            as Box<dyn RegeneratedDelta>),
        _ => None,
    };
    BatchOutcome::Ran {
        request_id,
        regenerate,
        check,
        delta,
    }
}

fn describe_failure(failure: &CheckExecutionFailure) -> String {
    match failure {
        CheckExecutionFailure::NotPlaceable(why) => why.clone(),
        CheckExecutionFailure::Process(error) => error.clone(),
        CheckExecutionFailure::Substrate(failure) => format!(
            "the substrate failed ({:?}): {}",
            failure.shape(),
            failure.diagnostic()
        ),
        CheckExecutionFailure::Suppressed(_) => {
            "submission declined: this batch's retry budget at this commit is spent".to_string()
        }
        CheckExecutionFailure::ConfigUnreadable(why) => {
            format!("the project's execution policy does not load: {why}")
        }
        CheckExecutionFailure::Cancelled(cause) => format!("cancelled: {cause:?}"),
        CheckExecutionFailure::LeftRunning(_) => {
            "the runner stopped waiting while the machine still owned the batch".to_string()
        }
    }
}

/// A fleet cell's delta, sealed through the same publication path a
/// write-check fix folds through.
struct FleetDelta {
    store_dir: PathBuf,
    request: cairn_common::executor_protocol::CellRequest,
    delta: crate::fleet::MutationDelta,
}

impl RegeneratedDelta for FleetDelta {
    fn seal<'a>(
        self: Box<Self>,
        orch: &'a Orchestrator,
        branch: &'a str,
        message: &'a str,
        admits: &'a (dyn Fn(&str) -> bool + Send + Sync),
    ) -> BoxFuture<'a, Result<Option<String>, String>> {
        Box::pin(async move {
            let author = cairn_vcs::PublicationIdentity::new(
                cairn_vcs::MANAGED_IDENTITY_NAME,
                cairn_vcs::MANAGED_IDENTITY_EMAIL,
            );
            crate::mcp::handlers::run::publish_and_seal_slot_delta_within(
                orch,
                &self.store_dir,
                &self.request,
                &self.delta,
                branch,
                message,
                Some(&author),
                Some(admits),
            )
            .await
            .map(|published| published.map(|published| published.commit))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const PROJECT: &str = "proj-1";
    const BRANCH: &str = "agent/regen";

    async fn db(name: &str) -> Arc<LocalDb> {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, \
             created_at, updated_at) VALUES ('proj-1', 'default', 'P', 'proj', '/r', 'main', 1, 1);"
                .to_string(),
        )
        .await
        .unwrap();
        Arc::new(db)
    }

    fn rule(glob: &str) -> LandingRegenerateRule {
        LandingRegenerateRule {
            paths: vec![glob.to_string()],
            regenerate: "derive".into(),
            check: "derive --check".into(),
            executor: None,
            timeout: Some(1),
            lane: Some("uri-wasm".into()),
        }
    }

    fn stale(path: &str) -> Vec<StalePath> {
        vec![StalePath {
            path: path.into(),
            blob: Some("b".into()),
        }]
    }

    async fn install(db: &LocalDb, globs: &[&str]) -> Vec<i64> {
        let rules: Vec<LandingRegenerateRule> = globs.iter().map(|glob| rule(glob)).collect();
        let paired: Vec<_> = rules
            .iter()
            .map(|rule| (rule, stale(&rule.paths[0].replace("**", "out"))))
            .collect();
        record_install(db, PROJECT, Path::new("/s"), BRANCH, &paired, "h", "b")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_installs_rules_are_recorded_together_and_admit_each_others_seals() {
        let db = db("regenerate-group.db").await;
        let ids = install(&db, &["gen-a/**", "gen-b/**"]).await;
        for id in &ids {
            assert!(
                load(&db, *id).await.unwrap().unwrap().holds(),
                "no sibling superseded"
            );
        }
        mark_running(&db, ids[0]).await.unwrap();
        record_request(&db, ids[0], "request-a").await.unwrap();
        let own = batch_job_id(ids[0]);
        assert!(
            holding_except(&db, PROJECT, BRANCH, Some((&own, "request-a")))
                .await
                .unwrap()
                .is_none(),
            "the first rule's seal is admitted past its sibling's hold"
        );
        assert!(holding(&db, PROJECT, BRANCH).await.unwrap().is_some());
        assert!(
            holding_except(&db, PROJECT, BRANCH, Some((&own, "another-request")))
                .await
                .unwrap()
                .is_some(),
            "only that batch's own seal"
        );
        // A later install of the branch replaces both.
        install(&db, &["gen-a/**"]).await;
        for id in &ids {
            assert_eq!(load(&db, *id).await.unwrap().unwrap().state, SUPERSEDED);
        }
    }

    /// The landing's own preparation recorded rows and refused; they settle
    /// before the loop looks, and the landing is retried all the same.
    async fn lands_after_rows_that_settled_before_the_wait(state: &'static str) -> Vec<String> {
        let db = db(&format!("regenerate-merge-{state}.db")).await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let (result, notes) = land_after_regenerations(&db, PROJECT, BRANCH, || {
            let (db, attempts) = (db.clone(), attempts.clone());
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) > 0 {
                    return Ok("landed".to_string());
                }
                let ids = install(&db, &["gen/**"]).await;
                let failure = RegenerationFailure {
                    command: "derive".into(),
                    exit: Some(1),
                    output_tail: String::new(),
                    reason: None,
                };
                let (commit, failure) = match state {
                    SUCCEEDED => (Some("c1"), None),
                    _ => (None, Some(&failure)),
                };
                assert!(settle(&db, ids[0], state, commit, failure).await.unwrap());
                Err("busy: regenerating".to_string())
            }
        })
        .await
        .unwrap();
        assert_eq!(result, Ok("landed".to_string()));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        notes
    }

    #[tokio::test]
    async fn a_merge_lands_after_a_regeneration_that_succeeded_before_it_looked() {
        let notes = lands_after_rows_that_settled_before_the_wait(SUCCEEDED).await;
        assert_eq!(notes.len(), 1);
        assert!(notes[0].contains("regenerated as `c1`"), "{notes:?}");
    }

    #[tokio::test]
    async fn a_merge_lands_after_a_regeneration_that_failed_before_it_looked() {
        let notes = lands_after_rows_that_settled_before_the_wait(FAILED).await;
        assert!(notes[0].contains("it failed"), "{notes:?}");
    }

    #[tokio::test]
    async fn a_refusal_that_recorded_no_regeneration_is_returned_as_it_is() {
        let db = db("regenerate-merge-none.db").await;
        let attempts = AtomicUsize::new(0);
        let (result, notes) = land_after_regenerations(&db, PROJECT, BRANCH, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<String, _>("conflicts with base".to_string()) }
        })
        .await
        .unwrap();
        assert_eq!(result, Err("conflicts with base".to_string()));
        assert!(notes.is_empty());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn only_the_rules_proving_lane_clears_a_failure() {
        let db = db("regenerate-clear.db").await;
        let ids = install(&db, &["packages/ui/src/uri-wasm/**"]).await;
        let row = load(&db, ids[0]).await.unwrap().unwrap();
        // `git-init-branch` and `dead-code` have impacts that include the
        // pair in this repository; their green proves nothing about it while
        // `uri-wasm` itself is red.
        let overlapping = ["git-init-branch".to_string(), "dead-code".to_string()];
        assert!(!cleared_by(&row, &overlapping));
        assert!(cleared_by(
            &row,
            &["git-init-branch".to_string(), "uri-wasm".to_string()]
        ));
        let mut unproven = row.clone();
        unproven.rule.lane = None;
        assert!(
            !cleared_by(&unproven, &["uri-wasm".to_string()]),
            "a rule naming no proving lane is cleared only by its own regeneration"
        );
        // Only a failed row clears.
        assert!(!clear(&db, row.id).await.unwrap());
        settle(&db, row.id, FAILED, None, None).await.unwrap();
        assert!(clear(&db, row.id).await.unwrap());
        assert!(stale_failures(&db, PROJECT, BRANCH)
            .await
            .unwrap()
            .is_empty());
    }

    fn row(project: &str, id: i64, group: &str) -> Regeneration {
        Regeneration {
            id,
            project_id: project.into(),
            store_path: "/s".into(),
            bookmark: BRANCH.into(),
            install_group: group.into(),
            rule: rule("gen/**"),
            stale: stale("gen/out"),
            installed_head: "h".into(),
            base_commit: "b".into(),
            state: PENDING.into(),
            request_id: None,
            failure: None,
            commit: None,
            dispatches: 0,
            created_ms: 0,
            updated_ms: 0,
        }
    }

    /// The local database and a team replica both number their rows from 1;
    /// recovering one database's row #1 must not pass for owning another's.
    #[test]
    fn an_orphan_is_owned_by_project_and_row_never_by_row_alone() {
        let owned = HashSet::from([("local-project".to_string(), 1)]);
        let orphans = orphan_installs(
            vec![
                row("local-project", 1, "local-install"),
                row("team-project", 1, "team-install"),
                row("team-project", 2, "team-install"),
            ],
            &owned,
        );
        assert_eq!(
            orphans,
            [(
                "team-project".to_string(),
                "team-install".to_string(),
                vec![1, 2]
            )]
        );
    }

    #[test]
    fn rows_that_run_one_after_another_are_waited_for_by_their_sum() {
        let rows = [row(PROJECT, 1, "g"), row(PROJECT, 2, "g")];
        assert_eq!(serial_bound(&rows), rows[0].settle_bound() * 2);
    }

    /// Two rows of one install settling one after another are waited past
    /// within the sum of their budgets, though not within one row's.
    #[tokio::test]
    async fn a_serial_group_settles_within_its_summed_budget() {
        let db = db("regenerate-serial-wait.db").await;
        let ids = install(&db, &["gen-a/**", "gen-b/**"]).await;
        let rows = vec![
            load(&db, ids[0]).await.unwrap().unwrap(),
            load(&db, ids[1]).await.unwrap().unwrap(),
        ];
        let step = Duration::from_millis(300);
        let settler = {
            let (db, ids) = (db.clone(), ids.clone());
            tokio::spawn(async move {
                for id in ids {
                    tokio::time::sleep(step).await;
                    settle(&db, id, SUCCEEDED, None, None).await.unwrap();
                    settled_signal(PROJECT, BRANCH).notify_waiters();
                }
            })
        };
        let refused = wait_for_rows_within(&db, &rows, step + step / 2)
            .await
            .unwrap_err();
        assert!(refused.contains("is still regenerating"), "{refused}");
        let settled = wait_for_rows_within(&db, &rows, step * 4).await.unwrap();
        assert!(settled.iter().all(|row| row.state == SUCCEEDED));
        settler.await.unwrap();
    }
}
