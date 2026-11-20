//! Start now: the one way past every cap, whatever holds the child.
//!
//! A child waits either as a held execution (capacity gate rows, no jobs yet)
//! or, once its execution has started, as a job its thread's bound holds (a
//! `thread_open_prs` or `thread_focus` row in `provider_limit_holds`). A Start
//! now releases whichever it is, records every cap that actually held the work
//! in the execution's `release_override_json.skipped`, and then reads what the
//! launch came to, so the surface where it was pressed says so at once: it
//! launched, it is held for supply, its launch was refused, or it had already
//! started.
//!
//! A job the start releases launches through the ordinary funnel. Admission
//! reads the operator's record on the execution and admits a never-run job
//! past its thread's bound (see `skip_bound_for_operator_start` in
//! `orchestrator::wakes::provider_limits`), so no later step re-holds it.

use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

use super::sweep::{self, CapSkipAuthority};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

/// How long a Start now waits for its launch to report before answering that
/// the launch is under way. A unit test has no process to launch, so it would
/// only ever wait the bound out; tests that read a verdict pass their own wait
/// to [`start_now_within`].
#[cfg(not(test))]
const VERDICT_WAIT: Duration = Duration::from_secs(15);
#[cfg(test)]
const VERDICT_WAIT: Duration = Duration::ZERO;
const VERDICT_POLL: Duration = Duration::from_millis(250);

/// What a Start now came to, as read once the launch reported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum StartOutcome {
    /// A node launched: a run exists for it.
    Launched { node: String },
    /// Released from every cap, but no account or model can serve it now.
    HeldForSupply { reason: String },
    /// Released, but its launch was refused or failed, and why.
    Refused { reason: String },
    /// Nothing was waiting: it had already started.
    AlreadyStarted,
    /// Released, and its launch has not reported within the wait.
    Starting,
}

impl StartOutcome {
    /// Whether the child is now on its way to running.
    pub fn started(&self) -> bool {
        matches!(self, Self::Launched { .. } | Self::Starting)
    }

    /// The sentence the pressed button, the toast and an MCP reply all show.
    pub fn sentence(&self, label: &str) -> String {
        match self {
            Self::Launched { node } => format!("Started {label}: its {node} launched."),
            Self::HeldForSupply { reason } => format!(
                "Released {label} from every cap, but it cannot run yet: {reason}. It starts \
                 when an account can serve it."
            ),
            Self::Refused { reason } => {
                format!("Released {label}, but its launch was refused: {reason}")
            }
            Self::AlreadyStarted => {
                format!("{label} has already started; nothing was waiting to start.")
            }
            Self::Starting => {
                format!("Started {label}: its launch is under way and has not reported yet.")
            }
        }
    }
}

/// Start the held work of `execution_id` now, past every cap, on `by`'s
/// authority, and report what its launch came to.
pub async fn start_now(
    orch: &Orchestrator,
    execution_id: &str,
    principal: Value,
    by: CapSkipAuthority,
) -> Result<StartOutcome, String> {
    start_now_within(orch, execution_id, principal, by, VERDICT_WAIT).await
}

pub(crate) async fn start_now_within(
    orch: &Orchestrator,
    execution_id: &str,
    principal: Value,
    by: CapSkipAuthority,
    wait: Duration,
) -> Result<StartOutcome, String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id)
        .await
        .map_err(|error| error.to_string())?;
    let since = orch.services.clock.now();
    let holds = super::open_prs::thread_holds(&db, execution_id)
        .await
        .map_err(|error| error.to_string())?;
    if holds.is_empty() {
        // An override release licenses any unreleased row, and an execution
        // that was never gated has none to release: it started on filing.
        if !gated_unreleased(&db, execution_id).await? {
            return not_held(&db, execution_id).await;
        }
        let outcome = sweep::release(
            orch,
            execution_id,
            sweep::ReleaseCause::Override {
                principal,
                scope: sweep::OverrideScope::Everything { by },
            },
        )
        .await?;
        match outcome {
            sweep::ReleaseOutcome::Released => {}
            sweep::ReleaseOutcome::NotHeld => return not_held(&db, execution_id).await,
            sweep::ReleaseOutcome::HeldByCap(_) => {
                return Err("a Start now was held by a cap it should have skipped".to_string())
            }
        }
        let jobs = execution_jobs(&db, execution_id).await?;
        await_verdict(&db, &jobs, since, wait).await
    } else {
        let record = held_job_record(&db, execution_id, principal, &by, &holds, since).await?;
        let mut released = Vec::new();
        for hold in &holds {
            if crate::orchestrator::wakes::start_now_thread_hold(orch, &hold.id, record.clone())
                .await?
            {
                released.push(hold.job_id.clone());
            }
        }
        if released.is_empty() {
            return not_held(&db, execution_id).await;
        }
        await_verdict(&db, &released, since, wait).await
    }
}

/// `execution 2 on cairn://p/cairn/7360`: how a Start now's sentence names the
/// execution it acted on.
pub async fn label(db: &LocalDb, execution_id: &str) -> String {
    let found = db
        .query_opt(
            "SELECT e.seq, p.key, i.number FROM executions e
               JOIN issues i ON i.id = e.issue_id JOIN projects p ON p.id = i.project_id
              WHERE e.id = ?1",
            (execution_id.to_string(),),
            |row| Ok((row.opt_i64(0)?, row.text(1)?, row.i64(2)?)),
        )
        .await
        .ok()
        .flatten();
    match found {
        Some((seq, key, number)) => {
            let issue =
                cairn_uri::build_issue_uri(&cairn_uri::canonical_project(&key), number as i32);
            match seq {
                Some(seq) => format!("execution {seq} on {issue}"),
                None => format!("the execution on {issue}"),
            }
        }
        None => "the execution".to_string(),
    }
}

/// The override a Start now of held jobs records: who, when, on whose
/// authority, and every cap that held the work. The caps an earlier start of
/// this execution skipped are kept, since that start is part of how it came to
/// run.
async fn held_job_record(
    db: &LocalDb,
    execution_id: &str,
    principal: Value,
    by: &CapSkipAuthority,
    holds: &[super::open_prs::ThreadHold],
    now: i64,
) -> Result<Value, String> {
    let earlier = db
        .query_opt_text(
            "SELECT release_override_json FROM executions WHERE id = ?1",
            (execution_id.to_string(),),
        )
        .await
        .map_err(|error| error.to_string())?
        .and_then(|json| serde_json::from_str::<Value>(&json).ok());
    let mut skipped: Vec<String> = earlier
        .as_ref()
        .and_then(|record| record.get("skipped"))
        .and_then(Value::as_array)
        .map(|labels| {
            labels
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    for label in holds
        .iter()
        .filter_map(|hold| super::open_prs::skipped_label(&hold.boundary))
    {
        if !skipped.iter().any(|known| known == label) {
            skipped.push(label.to_string());
        }
    }
    let mut record = serde_json::json!({
        "principal": principal,
        "at": now,
        "operator": matches!(by, CapSkipAuthority::Operator),
        "skipped": skipped,
    });
    if let CapSkipAuthority::Granted { thread, decision } = by {
        record["granted"] = serde_json::json!({ "thread": thread, "decision": decision });
    }
    Ok(record)
}

/// Whether the execution waits as a held execution: gated and not released.
async fn gated_unreleased(db: &LocalDb, execution_id: &str) -> Result<bool, String> {
    db.query_opt(
        concat!(
            "SELECT 1 FROM executions e WHERE e.id = ?1 AND ",
            cairn_db::gated_unreleased_sql!()
        ),
        (execution_id.to_string(),),
        |row| row.i64(0),
    )
    .await
    .map(|found| found.is_some())
    .map_err(|error| error.to_string())
}

/// Nothing was released: it had already started, or it can no longer start.
async fn not_held(db: &LocalDb, execution_id: &str) -> Result<StartOutcome, String> {
    let released = db
        .query_opt(
            "SELECT released_at IS NOT NULL OR NOT EXISTS (
                      SELECT 1 FROM execution_gates g WHERE g.execution_id = executions.id)
               FROM executions WHERE id = ?1",
            (execution_id.to_string(),),
            |row| row.i64(0),
        )
        .await
        .map_err(|error| error.to_string())?;
    match released {
        Some(1) => Ok(StartOutcome::AlreadyStarted),
        Some(_) => Err(
            "That execution can no longer start: it was superseded, or its issue is resolved."
                .to_string(),
        ),
        None => Err("That execution does not exist.".to_string()),
    }
}

/// Read what the launch of `jobs` came to until it reports or `wait` passes.
async fn await_verdict(
    db: &LocalDb,
    jobs: &[String],
    since: i64,
    wait: Duration,
) -> Result<StartOutcome, String> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        if let Some(outcome) = launch_verdict(db, jobs, since).await? {
            return Ok(outcome);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(StartOutcome::Starting);
        }
        tokio::time::sleep(VERDICT_POLL).await;
    }
}

/// What the launch of `jobs`, the jobs this Start released, has come to since
/// `since`, once the launch funnel has decided it. Nothing about any other job
/// of the execution is read, so a sibling's activity is never this Start's
/// verdict.
///
/// In order: an active supply hold (account routing refused it, which it may
/// do after preparation has already written a `starting` run, so a supply hold
/// outranks any run); a thread hold re-armed since the start; a turn that
/// started since the start, which is the only proof of a launch, since a turn
/// starts only once a process serves it, and a warm resume reuses its run
/// rather than inserting one; then a run that crashed, or a job that failed,
/// since the start. A `starting` run and a pending turn are an undecided
/// launch, not a verdict.
pub(crate) async fn launch_verdict(
    db: &LocalDb,
    jobs: &[String],
    since: i64,
) -> Result<Option<StartOutcome>, String> {
    if jobs.is_empty() {
        return Ok(None);
    }
    let jobs = jobs.to_vec();
    db.read(move |conn| {
        let jobs = jobs.clone();
        Box::pin(async move {
            use cairn_db::storage::QueryConn;
            use cairn_db::turso::Value as SqlValue;

            let n = jobs.len();
            let list = cairn_db::sql::placeholders(n);
            let since_at = n + 1;
            let bound = |with_since: bool| {
                let mut values: Vec<SqlValue> =
                    jobs.iter().map(|id| SqlValue::Text(id.clone())).collect();
                if with_since {
                    values.push(SqlValue::Integer(since));
                }
                values
            };

            let sql = format!(
                "SELECT h.boundary_source, COALESCE(h.observed_text, h.boundary_source), h.updated_at
                   FROM provider_limit_holds h
                  WHERE h.job_id IN ({list}) AND h.state = 'active'
                  ORDER BY h.updated_at DESC, h.id"
            );
            let mut rows = conn.query_rows(&sql, bound(false)).await?;
            let mut rearmed = None;
            while let Some(row) = rows.next().await? {
                let boundary = row.text(0)?;
                let reason = row.text(1)?;
                if !matches!(boundary.as_str(), "thread_open_prs" | "thread_focus") {
                    return Ok(Some(StartOutcome::HeldForSupply { reason }));
                }
                if row.i64(2)? >= since {
                    rearmed.get_or_insert(reason);
                }
            }
            drop(rows);
            if let Some(reason) = rearmed {
                return Ok(Some(StartOutcome::Refused {
                    reason: format!("its thread's bound held it again: {reason}"),
                }));
            }

            let sql = format!(
                "SELECT COALESCE(j.node_name, j.uri_segment, 'node')
                   FROM turns t JOIN jobs j ON j.id = t.job_id
                  WHERE t.job_id IN ({list}) AND t.started_at >= ?{since_at}
                  ORDER BY t.started_at, t.id LIMIT 1"
            );
            let mut rows = conn.query_rows(&sql, bound(true)).await?;
            if let Some(row) = rows.next().await? {
                return Ok(Some(StartOutcome::Launched { node: row.text(0)? }));
            }
            drop(rows);

            let sql = format!(
                "SELECT COALESCE(j.node_name, j.uri_segment, 'node'),
                        COALESCE(r.error_message, r.exit_reason)
                   FROM runs r JOIN jobs j ON j.id = r.job_id
                  WHERE r.job_id IN ({list}) AND r.status = 'crashed' AND r.updated_at >= ?{since_at}
                  ORDER BY r.updated_at DESC, r.id LIMIT 1"
            );
            let mut rows = conn.query_rows(&sql, bound(true)).await?;
            if let Some(row) = rows.next().await? {
                let node = row.text(0)?;
                let reason = row
                    .opt_text(1)?
                    .unwrap_or_else(|| format!("its {node} failed to start"));
                return Ok(Some(StartOutcome::Refused { reason }));
            }
            drop(rows);

            let sql = format!(
                "SELECT COALESCE(j.node_name, j.uri_segment, 'node') FROM jobs j
                  WHERE j.id IN ({list}) AND j.status = 'failed' AND j.updated_at >= ?{since_at}
                  LIMIT 1"
            );
            let mut rows = conn.query_rows(&sql, bound(true)).await?;
            if let Some(row) = rows.next().await? {
                let node = row.text(0)?;
                return Ok(Some(StartOutcome::Refused {
                    reason: format!("its {node} failed before it could start"),
                }));
            }
            Ok(None)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The jobs a released execution's Start answers for: every job its release
/// created, since an execution released just now has no other.
async fn execution_jobs(db: &LocalDb, execution_id: &str) -> Result<Vec<String>, String> {
    let execution = execution_id.to_string();
    db.read(move |conn| {
        let execution = execution.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM jobs WHERE execution_id = ?1 ORDER BY id",
                    params![execution.as_str()],
                )
                .await?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next().await? {
                jobs.push(row.text(0)?);
            }
            Ok(jobs)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Execution `e` with this Start's builder `j`, resting on head turn `t0`
    /// of run `r0` (created long before the start), and a sibling job `sib`.
    async fn seeded(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
               VALUES('i','p',1,'I','active',1,1);
             INSERT INTO executions(id,issue_id,project_id,recipe_id,status,started_at,seq,released_at)
               VALUES('e','i','p','build','running',1,1,5);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,status,node_name,uri_segment,recipe_node_id,created_at,updated_at)
               VALUES('j','p','i','e','pending','builder','builder','builder',1,1),
                     ('sib','p','i','e','running','reviewer','reviewer','reviewer',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('s','j','claude','native','open',1,1,1),
                     ('ss','sib','claude','native2','open',1,1,1);
             INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('r0','p','i','j','s','live','fresh',1,1),
                     ('rs','p','i','sib','ss','live','fresh',11,11);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at,started_at)
               VALUES('t0','s','r0','j',1,'completed','initial',1,1,1),
                     ('ts','ss','rs','sib',1,'running','initial',11,11,11);
             UPDATE jobs SET current_turn_id='t0', current_session_id='s' WHERE id='j';",
        )
        .await
        .unwrap();
        db
    }

    fn this_start() -> Vec<String> {
        vec!["j".to_string()]
    }

    /// A launch is decided only by the funnel: a `starting` run written by
    /// preparation is not a launch, a supply hold armed after it outranks it,
    /// a sibling's started turn is never this Start's verdict, and a warm
    /// resume, which reuses its run, launches when its turn starts.
    #[tokio::test]
    async fn only_a_started_turn_of_the_released_job_is_a_launch() {
        let db = seeded("start_now_verdict").await;
        assert_eq!(
            launch_verdict(&db, &this_start(), 10).await.unwrap(),
            None,
            "a sibling's turn started since the start is not this Start's verdict"
        );

        db.execute_script(
            "INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('r1','p','i','j','s','starting','fresh',11,11);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('t1','s','r1','j',2,'pending','initial',11,11);",
        )
        .await
        .unwrap();
        assert_eq!(
            launch_verdict(&db, &this_start(), 10).await.unwrap(),
            None,
            "preparation's starting run and pending turn are an undecided launch"
        );

        db.execute_script(
            "INSERT INTO provider_limit_holds(id,job_id,backend,boundary_source,observed_text,kind,created_at,updated_at)
               VALUES('supply','j','claude','recheck','claude/opus cannot run now: every Claude account is out of usage','launch',12,12);",
        )
        .await
        .unwrap();
        assert_eq!(
            launch_verdict(&db, &this_start(), 10).await.unwrap(),
            Some(StartOutcome::HeldForSupply {
                reason: "claude/opus cannot run now: every Claude account is out of usage".into()
            }),
            "account routing's refusal after preparation outranks the starting run"
        );

        // The warm-resume shape: no new run, the head's successor turn starts
        // on the run the session already had.
        db.execute_script(
            "UPDATE provider_limit_holds SET state='resolved' WHERE id='supply';
             DELETE FROM turns WHERE id='t1'; DELETE FROM runs WHERE id='r1';
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('t2','s','r0','j',2,'pending','resume',11,11);",
        )
        .await
        .unwrap();
        assert_eq!(launch_verdict(&db, &this_start(), 10).await.unwrap(), None);
        db.execute_script("UPDATE turns SET state='running', started_at=12 WHERE id='t2';")
            .await
            .unwrap();
        assert_eq!(
            launch_verdict(&db, &this_start(), 10).await.unwrap(),
            Some(StartOutcome::Launched {
                node: "builder".into()
            })
        );
        assert_eq!(
            launch_verdict(&db, &this_start(), 20).await.unwrap(),
            None,
            "a turn that started before the start is not its verdict"
        );
    }

    /// A startup that dies after preparation (the process never served its
    /// turn) and a thread hold armed again since the start are refusals.
    #[tokio::test]
    async fn a_delayed_startup_failure_or_a_rearmed_bound_is_a_refusal() {
        let db = seeded("start_now_refused").await;
        db.execute_script(
            "INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('r1','p','i','j','s','starting','fresh',11,11);",
        )
        .await
        .unwrap();
        assert_eq!(launch_verdict(&db, &this_start(), 10).await.unwrap(), None);
        db.execute_script(
            "UPDATE runs SET status='crashed', error_message='branch refused: base moved',
                             exit_reason='session_start_failed', updated_at=14 WHERE id='r1';",
        )
        .await
        .unwrap();
        assert_eq!(
            launch_verdict(&db, &this_start(), 10).await.unwrap(),
            Some(StartOutcome::Refused {
                reason: "branch refused: base moved".into()
            })
        );

        let db = seeded("start_now_rearmed").await;
        db.execute_script(
            "INSERT INTO provider_limit_holds(id,job_id,backend,boundary_source,observed_text,kind,created_at,updated_at)
               VALUES('old','j','claude','thread_open_prs','execution has 6/6 open PRs','launch',1,1);",
        )
        .await
        .unwrap();
        assert_eq!(
            launch_verdict(&db, &this_start(), 10).await.unwrap(),
            None,
            "the thread hold from before the start is the one it released"
        );
        db.execute_script("UPDATE provider_limit_holds SET updated_at=12 WHERE id='old';")
            .await
            .unwrap();
        assert!(matches!(
            launch_verdict(&db, &this_start(), 10).await.unwrap(),
            Some(StartOutcome::Refused { reason }) if reason.contains("6/6 open PRs")
        ));
    }

    /// The funnel's own no-supply transition, on the shape preparation leaves
    /// (a starting run and a pending initial turn): the hold it arms is read
    /// as held for supply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_funnels_no_supply_hold_after_preparation_reads_as_held_for_supply() {
        let db = std::sync::Arc::new(seeded("start_now_funnel_no_supply").await);
        db.execute_script(
            "INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('r1','p','i','j','s','starting','fresh',11,11);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('t1','s','r1','j',2,'pending','initial',11,11);",
        )
        .await
        .unwrap();
        let owning = db.clone();
        tokio::task::spawn_blocking(move || {
            crate::execution::jobs::hold_refused_launch(
                &owning,
                "j",
                crate::orchestrator::wakes::HoldKind::Launch,
                Some("r1"),
                None,
                &cairn_models::supply::CapacityRefusal {
                    subject: "claude/opus".into(),
                    reason: "claude/opus cannot run now: every Claude account is out of usage"
                        .into(),
                    retry_at: None,
                    retry_account: None,
                },
                None,
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            launch_verdict(&db, &this_start(), 10).await.unwrap(),
            Some(StartOutcome::HeldForSupply {
                reason: "claude/opus cannot run now: every Claude account is out of usage".into()
            })
        );
    }

    #[test]
    fn each_outcome_says_what_happened_and_none_blames_a_race() {
        let label = "execution 1 on cairn://p/cairn/7360";
        let sentences = [
            StartOutcome::Launched {
                node: "builder".into(),
            },
            StartOutcome::HeldForSupply {
                reason: "no account can serve claude/opus".into(),
            },
            StartOutcome::Refused {
                reason: "branch refused".into(),
            },
            StartOutcome::AlreadyStarted,
            StartOutcome::Starting,
        ]
        .map(|outcome| outcome.sentence(label));
        for sentence in &sentences {
            assert!(sentence.contains(label), "{sentence}");
            assert!(!sentence.contains("someone else"), "{sentence}");
        }
        assert!(sentences[0].contains("builder launched"));
        assert!(sentences[1].contains("no account can serve claude/opus"));
        assert!(sentences[2].contains("branch refused"));
    }
}
