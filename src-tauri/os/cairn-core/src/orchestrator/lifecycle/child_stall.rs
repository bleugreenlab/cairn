//! Parent attention for a child that still claims to be working while holding
//! nothing that could do the work.
//!
//! ## The failure this names
//!
//! CAIRN-4602's builder lost session after session to provider silence over
//! twenty-two hours. Watchdog recovery terminalized each one with
//! `provider_silence_already_terminal_reconciled` and then either minted no
//! successor at all or minted one that died inside two minutes. Between the
//! first death and the second birth the job held no live session for seventeen
//! hours while still reading as running to every surface that asked. Its parent
//! learned nothing until the hard failure at the end, and the operator found and
//! nudged the child twice before any machinery said a word.
//!
//! The existing child-attention facts — question, permission, review, resolved,
//! message — all describe a child that is *doing* something. None of them
//! describes a child that has stopped being able to. This module is that fact.
//!
//! ## Observation, never authorization
//!
//! Nothing here starts a session, reserves a successor, or writes to the child's
//! job at all. Restart lives in [`super::watchdog_recovery`] under its own
//! budget; a second mechanism minting sessions from a different vantage point is
//! exactly the loop this must not become. The parent is told, and the parent
//! decides.
//!
//! ## Evidence, not elapsed silence
//!
//! Every term of the predicate is a durable row, because "quiet for a while" is
//! also what a warm idle node, a node waiting on a user, a node queued for an
//! executor, and a node in the middle of a long turn all look like. The entry
//! condition is a *recovery death*: a session this node owned, closed by the
//! watchdog with [`ALREADY_TERMINAL_RECONCILED_REASON`]. A node that has never
//! had one is never a candidate however long it has been silent.
//!
//! ## The death has to be the CURRENT loss
//!
//! "Has ever had a recovery death" is not the same claim as "is stalled by one",
//! and treating them as one would make candidacy permanent: a node that lost a
//! session, recovered, and worked healthily for weeks would still be admitted,
//! and whatever ended it later would be reported under the old death's name.
//! Three gates in [`evaluate`] tie the death to now, and each excludes a
//! different way a node moves past one:
//!
//! - the death must be the node's **newest terminal session transition** — a
//!   node whose latest session ended some other way has left the episode;
//! - it must not **predate the session the node currently holds** — a node whose
//!   current session outlived the death has already recovered from it;
//! - the node's job must still be **nonterminal**. A child that hard-failed is
//!   reported by its own resolution, and this fact exists for the state that
//!   path cannot describe. CAIRN-4602 shows both edges: the seventeen hours it
//!   spent `running` with a dead session are what went unreported, and the hard
//!   failure at the end is where the existing machinery finally spoke.
//!
//! ## Recovery is allowed to decline, not just to fail
//!
//! A grace after the death covers recovery *trying*. It does not cover recovery
//! deliberately **not** trying, which is a normal path: after terminalizing a
//! session, `recover_provider_watchdog` mints no successor when the job has a
//! pending resume trigger — an unanswered question, a queued message, a live
//! sub-task — because that trigger will resume it. Such a node sits
//! session-less on purpose, and reporting it would be both a false positive and
//! a false statement, since the notice says recovery has had its attempts. The
//! sweep asks recovery's own predicate rather than a second copy of it.
//!
//! ## Session status is evidence, not authority
//!
//! The viability half is [`classify`], and the reason it is not simply "is there
//! an open session row" is CAIRN-4602's own final state: the dead job's last two
//! sessions are *both* still `status='open'` with a NULL terminal reason, one of
//! them replaced by the other, and the job that owns them is `failed`. An open
//! row proves a row was written, not that anything can resume through it. So a
//! replaced session is not viable, and a session whose owning job is terminal is
//! not viable, whatever the row says about itself.
//!
//! (That those rows are left open at replacement and at job failure is a
//! bookkeeping defect in its own right — `sessions::queries::rotate_session`
//! marks the predecessor replaced without closing it, and job failure closes
//! nothing. It is separable from parent attention, so it is filed as CAIRN-4613
//! rather than repaired here, and this module reads the rows as they are. When
//! that lands, the two arms below stop occurring in new data but must stay for
//! old rows.)

use crate::orchestrator::attention_push::{
    latest_push_fingerprint, push_with_fingerprint, Boundary, Wake,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

use super::watchdog_recovery::{
    has_pending_resume_trigger_async, ALREADY_TERMINAL_RECONCILED_REASON,
};

/// The push key prefix. The suffix is the child ISSUE uri, which is what an
/// issue subscription stores as its `source_ref` — so a parent that muted the
/// child gets a passive ride-along instead of a rousing wake, through the same
/// central downgrade every other issue-sourced push takes.
pub(crate) use cairn_attention::push::CHILD_STALL_PUSH_PREFIX;

/// How long a node is left alone after a recovery death before its parent hears
/// about it.
///
/// Recovery is allowed to work. `sweep_stranded_watchdog_jobs` waits
/// `STRANDED_GRACE_SECS` (120s) before it will even try to mint a successor, and
/// the launch itself takes time, so anything shorter than that would report
/// every ordinary provider-silence recovery as a stall. Five minutes clears the
/// whole ceremony with room to spare while still being three orders of magnitude
/// faster than the twenty-two hours CAIRN-4602's parent actually waited.
const RECOVERY_GRACE_SECS: i64 = 5 * 60;

/// One recovery-terminalized session death, as the `sessions` row recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryDeath {
    pub(crate) sequence: i64,
    pub(crate) reason: String,
    pub(crate) closed_at: i64,
}

/// A node's current session — as much of the row as viability and scoping need,
/// and deliberately no more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentSession {
    pub(crate) id: String,
    pub(crate) status: String,
    pub(crate) replaced_by_id: Option<String>,
    /// When this session was born, which is what dates a recovery death as
    /// belonging to the node's present rather than to its history.
    pub(crate) created_at: i64,
}

/// Why a node that still claims to be working has no session that could do it.
///
/// The variants are the distinct shapes CAIRN-4602 went through, and they are
/// kept distinct because they call for different things from the reader: the
/// first says recovery gave up, the last says the node is already dead and its
/// session bookkeeping has not caught up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StallCause {
    /// The node's current session is terminal (or it has none), and nothing
    /// replaced it. The seventeen-hour shape.
    NoSurvivingSession,
    /// The current session reads `open` but has already been replaced, so it is
    /// a pointer to a conversation someone else superseded.
    SupersededSessionRetained { session_id: String },
}

impl StallCause {
    /// The stable tag that goes in the push fingerprint, so a stall that changes
    /// shape re-reaches a parent that was already told about the old one.
    fn tag(&self) -> &'static str {
        match self {
            StallCause::NoSurvivingSession => "no-surviving-session",
            StallCause::SupersededSessionRetained { .. } => "superseded-session-retained",
        }
    }
}

/// The viability predicate: `None` when this node holds a session that could
/// actually resume, `Some(cause)` when it does not.
///
/// Pure, and the whole of the session-side judgement — the queries around it
/// only fetch its argument. Read top to bottom it is three questions, each one a
/// way a session stops being able to carry a turn:
///
/// 1. Is there a current session at all?
/// 2. Is it open?
/// 3. Has something already replaced it?
///
/// Question 3 is the one a naive `status = 'open'` check skips, and it is why
/// this takes a whole row rather than a status: `open` is evidence that a row
/// was written, not authority that anything can resume through it. CAIRN-4602
/// ended with two such rows, one of them replaced by the other.
///
/// (The fourth question that state also raises — is the owning job still alive
/// — is asked by [`evaluate`]'s gate instead of here. A dead job is not a node
/// with an unusable session; it is a node that is over.)
pub(crate) fn classify(current: Option<&CurrentSession>) -> Option<StallCause> {
    let Some(current) = current else {
        // No session pointer at all. For a node with no recovery death on record
        // this is the not-yet-placed shape and never reaches here; for one with
        // a death it is loss.
        return Some(StallCause::NoSurvivingSession);
    };
    if current.status != "open" {
        return Some(StallCause::NoSurvivingSession);
    }
    if current.replaced_by_id.is_some() {
        return Some(StallCause::SupersededSessionRetained {
            session_id: current.id.clone(),
        });
    }
    None
}

/// One child node with no viable session, with the lifecycle evidence that says
/// so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StalledChild {
    pub(crate) job_id: String,
    pub(crate) issue_id: String,
    pub(crate) issue_uri: String,
    pub(crate) node_uri: String,
    pub(crate) cause: StallCause,
    /// Every recovery death this node has suffered, oldest first. Never empty:
    /// one death is the entry condition.
    pub(crate) deaths: Vec<RecoveryDeath>,
}

impl StalledChild {
    fn latest_death(&self) -> &RecoveryDeath {
        self.deaths
            .last()
            .expect("a stalled child is only built from at least one recovery death")
    }

    /// The content key of this stall. Two sweeps over an unchanged stall produce
    /// the same fingerprint and therefore one push and one wake, however long it
    /// lasts; another death, or a change of shape, produces a new one and the
    /// parent hears the update. Nothing derived from the current time appears
    /// here, which is what makes the wake count bounded by deaths rather than by
    /// sweeps.
    pub(crate) fn fingerprint(&self) -> String {
        format!(
            "{}:{}:{}",
            self.cause.tag(),
            self.deaths.len(),
            self.latest_death().closed_at
        )
    }

    /// The fact in words, as the parent reads it.
    ///
    /// `now` is a parameter rather than a call to the process clock so the
    /// rendered spans can be pinned in a test.
    pub(crate) fn render(&self, now: i64) -> String {
        let latest = self.latest_death();
        let mut out = format!(
            "Child `{}` has no viable session, and {} is still open.\n\n{}",
            self.node_uri,
            self.issue_uri,
            self.cause_sentence(latest, now)
        );
        if self.deaths.len() > 1 {
            let first = &self.deaths[0];
            out.push_str(&format!(
                " {} of its sessions have ended that way over {}, the first of them {}.",
                self.deaths.len(),
                crate::clock::format_elapsed(latest.closed_at.saturating_sub(first.closed_at)),
                crate::clock::host().age(first.closed_at, now),
            ));
        }
        out.push_str(
            "\n\nNothing will start another session for it on this account: provider-silence \
             recovery has already had its attempts, and this notice does not spend another. \
             Read the node and decide what the work needs.",
        );
        out
    }

    fn cause_sentence(&self, latest: &RecoveryDeath, now: i64) -> String {
        let ended = crate::clock::host().age(latest.closed_at, now);
        match &self.cause {
            StallCause::NoSurvivingSession => format!(
                "Every session it has held is terminal and none replaced the last one. \
                 Session {} ended {ended} — `{}`.",
                latest.sequence, latest.reason
            ),
            StallCause::SupersededSessionRetained { session_id } => format!(
                "Its current session `{session_id}` still reads `open`, but another session \
                 has already replaced it, so nothing can resume through it. \
                 Session {} ended {ended} — `{}`.",
                latest.sequence, latest.reason
            ),
        }
    }
}

/// The candidate statement, driven from `sessions` rather than from `jobs`.
///
/// The entry evidence is a recovery death, and that is a `sessions` row, so this
/// scans `sessions` once under an equality predicate and reaches everything else
/// by index seek off the few rows that match. Driving from `jobs` instead —
/// which reads more naturally, since a job is the subject — plans as a scan of
/// `jobs` with a correlated `sessions` seek per row: on a live workspace that is
/// sixteen thousand index seeks per sweep against one scan and a couple of
/// hundred. The subject of the query and the thing it should be driven by are
/// not the same, and the planner has no statistics to work that out on its own.
const CANDIDATE_SQL: &str = "SELECT DISTINCT j.id, j.status, j.current_session_id,
                                    i.id, p.key, i.number, e.seq, j.uri_segment
       FROM sessions s
       JOIN jobs j ON j.id = s.job_id
       JOIN issues i ON i.id = j.issue_id
       JOIN projects p ON p.id = i.project_id
       JOIN executions e ON e.id = j.execution_id
      WHERE s.terminal_reason = ?1
        AND s.closed_at IS NOT NULL
        AND j.parent_job_id IS NULL
        AND (i.parent_issue_id IS NOT NULL OR i.parent_thread_id IS NOT NULL)
        AND i.status NOT IN ('merged', 'closed', 'failed')";

/// One candidate node, before viability is decided.
struct Candidate {
    job_id: String,
    job_status: String,
    current_session_id: Option<String>,
    issue_id: String,
    issue_uri: String,
    node_uri: String,
}

/// Every child node in `db` that currently has no viable session.
///
/// The three gates, in the order they are cheapest to apply: a recovery death on
/// record (in [`CANDIDATE_SQL`], which is also what makes the candidate set
/// small), the grace since that death, and the viability predicate.
///
/// `parent_job_id IS NULL` scopes this to a child issue's own nodes. A sub-agent
/// task that loses its session is its parent NODE's problem and is felt as a
/// task that never returns; routing it to the grandparent issue's coordinator
/// would address it to someone with no standing to act on it.
pub(crate) async fn stalled_children(db: &LocalDb, now: i64) -> Result<Vec<StalledChild>, String> {
    let candidates = db
        .query_all(
            CANDIDATE_SQL,
            (ALREADY_TERMINAL_RECONCILED_REASON,),
            |row| {
                let project_key = row.text(4)?;
                let number = row.i64(5)? as i32;
                let exec_seq = row.i64(6)? as i32;
                let segment = row.opt_text(7)?;
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.opt_text(2)?,
                    row.text(3)?,
                    cairn_common::uri::build_issue_uri(&project_key, number),
                    segment.map(|segment| {
                        cairn_common::uri::build_node_uri(&project_key, number, exec_seq, &segment)
                    }),
                ))
            },
        )
        .await
        .map_err(|error| format!("stalled-child sweep failed to load candidates: {error}"))?;

    let mut stalled = Vec::new();
    for (job_id, job_status, current_session_id, issue_id, issue_uri, node_uri) in candidates {
        // A node with no `uri_segment` cannot be addressed, and an unaddressable
        // referent is worse than no notice: the parent would be woken and handed
        // nothing to read.
        let Some(node_uri) = node_uri else { continue };
        let candidate = Candidate {
            job_id,
            job_status,
            current_session_id,
            issue_id,
            issue_uri,
            node_uri,
        };
        if let Some(child) = evaluate(db, candidate, now).await? {
            stalled.push(child);
        }
    }
    Ok(stalled)
}

/// Decide one candidate.
///
/// The gates are ordered so the cheap disqualifiers run before the queries that
/// would answer the expensive ones, and each is a distinct claim rather than a
/// tuning knob:
///
/// 1. **A terminal job is out of scope.** Its parent hears about it through the
///    resolution path.
/// 2. **There is a recovery death**, and the grace since it has passed, so
///    recovery had room to work.
/// 3. **That death is the node's newest terminal session transition.** A node
///    whose latest session ended some other way has left the episode, and
///    naming this death as its latest would be a false statement as well as a
///    false positive.
/// 4. **The death does not predate the session the node currently holds.** A
///    node whose current session outlived the death has already recovered from
///    it.
/// 5. **Recovery was not declining on purpose** — a job with a pending resume
///    trigger is waiting, and it is waiting because recovery decided it should.
/// 6. **The session it holds cannot work** ([`classify`]).
async fn evaluate(
    db: &LocalDb,
    candidate: Candidate,
    now: i64,
) -> Result<Option<StalledChild>, String> {
    // A terminal child is out of scope for this fact entirely, rather than being
    // a stall of its own kind. Its parent hears about it through the resolution
    // path, and reporting it here as well would name whatever recovery death
    // preceded it as the cause of a death it did not cause.
    if crate::orchestrator::wakes::job_is_terminal(&candidate.job_status) {
        return Ok(None);
    }
    let deaths = recovery_deaths(db, &candidate.job_id).await?;
    let Some(latest) = deaths.last() else {
        return Ok(None);
    };
    if now.saturating_sub(latest.closed_at) < RECOVERY_GRACE_SECS {
        return Ok(None);
    }
    if newest_terminal_reason(db, &candidate.job_id)
        .await?
        .as_deref()
        != Some(ALREADY_TERMINAL_RECONCILED_REASON)
    {
        return Ok(None);
    }
    let current = match candidate.current_session_id.as_deref() {
        Some(session_id) => current_session(db, session_id).await?,
        None => None,
    };
    if current
        .as_ref()
        .is_some_and(|current| latest.closed_at < current.created_at)
    {
        return Ok(None);
    }
    if has_pending_resume_trigger_async(db, &candidate.job_id).await? {
        return Ok(None);
    }
    let Some(cause) = classify(current.as_ref()) else {
        return Ok(None);
    };
    Ok(Some(StalledChild {
        job_id: candidate.job_id,
        issue_id: candidate.issue_id,
        issue_uri: candidate.issue_uri,
        node_uri: candidate.node_uri,
        cause,
        deaths,
    }))
}

/// Every recovery-terminalized session this job has held, oldest first.
async fn recovery_deaths(db: &LocalDb, job_id: &str) -> Result<Vec<RecoveryDeath>, String> {
    db.query_all(
        "SELECT sequence, terminal_reason, closed_at FROM sessions
          WHERE job_id = ?1 AND terminal_reason = ?2 AND closed_at IS NOT NULL
          ORDER BY closed_at ASC",
        (job_id.to_string(), ALREADY_TERMINAL_RECONCILED_REASON),
        |row| {
            Ok(RecoveryDeath {
                sequence: row.i64(0)?,
                reason: row.text(1)?,
                closed_at: row.i64(2)?,
            })
        },
    )
    .await
    .map_err(|error| format!("stalled-child sweep failed to load session deaths: {error}"))
}

async fn current_session(db: &LocalDb, session_id: &str) -> Result<Option<CurrentSession>, String> {
    db.query_opt(
        "SELECT id, status, replaced_by_id, created_at FROM sessions WHERE id = ?1 LIMIT 1",
        (session_id.to_string(),),
        |row| {
            Ok(CurrentSession {
                id: row.text(0)?,
                status: row.text(1)?,
                replaced_by_id: row.opt_text(2)?,
                created_at: row.i64(3)?,
            })
        },
    )
    .await
    .map_err(|error| format!("stalled-child sweep failed to load the current session: {error}"))
}

/// The reason on the node's most recently CLOSED session, whatever that reason
/// is. `None` when it has never closed one.
///
/// Deliberately unfiltered by reason: the question is whether the recovery death
/// is still the node's latest word about itself, and a filtered query could only
/// ever answer yes.
async fn newest_terminal_reason(db: &LocalDb, job_id: &str) -> Result<Option<String>, String> {
    db.query_opt(
        "SELECT terminal_reason FROM sessions
          WHERE job_id = ?1 AND closed_at IS NOT NULL
          ORDER BY closed_at DESC, rowid DESC LIMIT 1",
        (job_id.to_string(),),
        |row| row.opt_text(0),
    )
    .await
    .map(Option::flatten)
    .map_err(|error| format!("stalled-child sweep failed to load the newest transition: {error}"))
}

/// Tell the child's watchers, and hand back the ones a wake should rouse.
///
/// The recipient set is [`watcher_jobs_for_issue`], which resolves the node
/// currently driving the parent live on every call (CAIRN-3293) — so a parent
/// issue that gained a new coordinator execution inherits its children's stalls
/// with no hand-off step, and a superseded coordinator stops hearing them. Jobs
/// on the child's own issue are excluded: a node cannot act on its own death.
///
/// [`watcher_jobs_for_issue`]: crate::orchestrator::wakes::watcher_jobs_for_issue
pub(crate) async fn tell_watchers(
    db: &LocalDb,
    child: &StalledChild,
) -> Result<Vec<String>, String> {
    let key = format!("{CHILD_STALL_PUSH_PREFIX}:{}", child.issue_uri);
    let fingerprint = child.fingerprint();
    let watchers = crate::orchestrator::wakes::watcher_jobs_for_issue(db, &child.issue_uri).await?;
    let mut pushed = Vec::new();
    for recipient in watchers {
        if crate::orchestrator::attention_delivery::job_belongs_to_issue(
            db,
            &recipient,
            &child.issue_id,
        )
        .await?
        {
            continue;
        }
        // The change trigger. A stall persists for as long as nobody fixes it,
        // and the sweep re-observes it every tick; without this the parent would
        // be woken on every one of them.
        if let Some(Some(previous)) = latest_push_fingerprint(db, &recipient, &key)
            .await
            .map_err(|error| error.to_string())?
        {
            if previous == fingerprint {
                continue;
            }
        }
        let (_, effective) = push_with_fingerprint(
            db,
            &recipient,
            &child.node_uri,
            Wake::Wake,
            Boundary::Event,
            &key,
            Some(&fingerprint),
        )
        .await
        .map_err(|error| error.to_string())?;
        if effective.wakes_idle() {
            pushed.push(recipient);
        }
    }
    Ok(pushed)
}

/// The periodic pass: find every stalled child and tell its watchers. Returns
/// how many stalls produced a fresh notice, so the sweep log distinguishes a
/// healthy quiet tick from one that never ran.
pub(crate) async fn sweep_stalled_children(orch: &Orchestrator) -> usize {
    let now = chrono::Utc::now().timestamp();
    let mut told = 0;
    for db in orch.db.all_dbs().await {
        let stalled = match stalled_children(&db, now).await {
            Ok(stalled) => stalled,
            Err(error) => {
                log::error!("{error}");
                continue;
            }
        };
        for child in stalled {
            let recipients = match tell_watchers(&db, &child).await {
                Ok(recipients) => recipients,
                Err(error) => {
                    log::warn!(
                        "stalled-child notice for {} failed: {error}",
                        child.node_uri
                    );
                    continue;
                }
            };
            if recipients.is_empty() {
                continue;
            }
            told += 1;
            orch.notifier.emit_change("attention_pushes");
            log::info!(
                "stalled child {}: {} recovery death(s), telling {} watcher(s)",
                child.node_uri,
                child.deaths.len(),
                recipients.len()
            );
            for recipient in recipients {
                if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
                    orch,
                    &recipient,
                    crate::messages::queued::DeliveryUrgency::Steer,
                ) {
                    log::warn!(
                        "stalled-child wake for {} failed: {error}",
                        &recipient[..recipient.len().min(8)]
                    );
                }
            }
        }
    }
    told
}

/// The body a `child-stalled:` push renders at delivery.
///
/// Re-derived from the live rows rather than frozen at creation, for the same
/// reason the check-verdict branch beside it is: the parent may drain this hours
/// after it was queued, and by then the number of deaths may have grown — or the
/// child may have recovered, in which case saying so is the whole of what the
/// parent needs and sending it to read a node that is fine would waste a turn.
///
/// The database is resolved from the RECIPIENT rather than from the node URI:
/// the parent and the child share a project database, and the recipient is a
/// job id already in hand, so this needs no URI-to-job lookup to find the place
/// the lookup would happen.
pub(crate) async fn render_stall_push_body(
    orch: &Orchestrator,
    recipient_job_id: &str,
    node_uri: &str,
) -> Option<String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, recipient_job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let now = chrono::Utc::now().timestamp();
    let stalled = stalled_children(&db, now).await.ok()?;
    match stalled.into_iter().find(|child| child.node_uri == node_uri) {
        Some(child) => Some(child.render(now)),
        None => Some(format!(
            "Child `{node_uri}` was reported with no viable session. It is no longer in that \
             state — it has been given a session again, or its issue has resolved. Nothing is \
             owed here unless you want to check what it did in between."
        )),
    }
}
