//! Persistence boundary between the rollout schedule and executor directives.

use std::collections::HashMap;
use std::sync::Mutex;

use cairn_common::identity::PrincipalRef;
use cairn_config::drain::{
    AdmissionEffect, DrainOwner, RuntimeDrainContext, RuntimeDrainKind, RuntimeDrainToken,
};

use cairn_common::executor_protocol::{
    describe_clock, ResidencyHolder, ResidentNotice, ResidentProcessKind,
};

use super::rollout::{NoticeKind, OwnerNotice, Resident};
use super::{management, rollout::SelfUpdateDrain, FleetConfig};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

pub(super) struct SelfUpdateRuntime {
    pub(super) pass: Mutex<()>,
    boot_id: String,
    tokens: Mutex<HashMap<String, RuntimeDrainToken>>,
}

impl Default for SelfUpdateRuntime {
    fn default() -> Self {
        Self {
            pass: Mutex::new(()),
            boot_id: uuid::Uuid::new_v4().to_string(),
            tokens: Mutex::new(HashMap::new()),
        }
    }
}

/// Commit the proposed owned holds before applying the schedule or sending a
/// directive. The pass gate excludes a second proposal through application;
/// connection and rollout locks are never held across a settings write.
pub fn advance(orch: &Orchestrator, now_unix_ms: u64) -> Result<(), String> {
    if !orch.fleet.drain_projection_initialized() {
        return Err(match orch.fleet.drain_initialization_error() {
            Some(error) => format!("fleet initialization remains closed: {error}"),
            None => "fleet authoritative settings have not been initialized".into(),
        });
    }
    let notices = orch
        .fleet
        .advance_self_update_rollout(now_unix_ms, |required| {
            persist_required(orch, required, now_unix_ms)
        })?;
    if notices.is_empty() {
        return Ok(());
    }
    // Telling an owner reads the database and queues a message, so it runs
    // beside the pass rather than inside it. The update does not wait on it:
    // the restart ends the resident whether or not its owner was reached.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            let orch = orch.clone();
            handle.spawn(async move {
                for notice in notices {
                    let outcome = deliver_owner_notice(&orch, &notice, unix_now_ms()).await;
                    orch.fleet.note_resident_notice(&notice, outcome);
                }
            });
        }
        Err(error) => log::warn!(
            "self-update: {} resident notice(s) not delivered, no runtime to deliver them on: {error}",
            notices.len()
        ),
    }
    Ok(())
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// Who a resident belongs to, as far as the runner can reach them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ResidentOwner {
    /// The job the message goes to.
    job_id: Option<String>,
    /// The live terminal session the notice is also written into.
    session_id: Option<String>,
    /// The resident's own resource, for the message to link.
    uri: Option<String>,
}

/// The job whose branch a dev instance was built from, as the dev-instance
/// path itself finds a branch's owner.
async fn branch_owner_job(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
) -> Result<Option<String>, String> {
    let (project_id, branch) = (project_id.to_string(), branch.to_string());
    orch.db
        .local
        .read(|conn| {
            let project_id = project_id.clone();
            let branch = branch.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM jobs WHERE project_id = ?1 AND branch = ?2 ORDER BY created_at DESC LIMIT 1",
                        cairn_db::turso::params![project_id.as_str(), branch.as_str()],
                    )
                    .await?;
                rows.next().await?.map(|row| row.text(0)).transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())
}

/// The owner of a resident: for a terminal, the row its process key names
/// (the executor's key for a terminal is its session id); for a dev instance,
/// the job whose branch it was built from; for anything else, the job
/// holding the environment it runs in.
///
/// `Ok` with no job is a resident nobody owns. `Err` is a lookup that failed,
/// which the caller reports as not yet delivered so the notice is retried.
async fn resolve_owner(orch: &Orchestrator, resident: &Resident) -> Result<ResidentOwner, String> {
    let address = &resident.address;
    let holder_job = match &address.holder {
        Some(ResidencyHolder::Job { job_id }) => Some(job_id.clone()),
        _ => None,
    }
    .or_else(|| {
        address
            .owner
            .as_ref()
            .and_then(|owner| owner.job_id.clone())
    });
    let (job_id, session_id, path) = match &address.kind {
        Some(ResidentProcessKind::Terminal { slug }) => {
            let session = address.process_key.clone();
            let holder = address
                .holder
                .as_ref()
                .map(ResidencyHolder::storage_key)
                .unwrap_or_default();
            let slug_for_row = slug.clone();
            let row = orch
                .db
                .local
                .read(|conn| {
                    let session = session.clone();
                    let holder = holder.clone();
                    let slug = slug_for_row.clone();
                    Box::pin(async move {
                        let mut rows = conn
                            .query(
                                "SELECT job_id, session_id FROM job_terminals
                                 WHERE status IN ('running', 'recovering')
                                   AND (session_id = ?1 OR (residency_holder = ?2 AND slug = ?3))
                                 ORDER BY session_id = ?1 DESC, created_at DESC
                                 LIMIT 1",
                                cairn_db::turso::params![
                                    session.as_str(),
                                    holder.as_str(),
                                    slug.as_str()
                                ],
                            )
                            .await?;
                        match rows.next().await? {
                            Some(row) => Ok(Some((row.opt_text(0)?, row.text(1)?))),
                            None => Ok(None),
                        }
                    })
                })
                .await
                .map_err(|error| error.to_string())?;
            match row {
                Some((job, session)) => (
                    job.or(holder_job),
                    Some(session),
                    Some(format!("terminal/{slug}")),
                ),
                None => (holder_job, None, Some(format!("terminal/{slug}"))),
            }
        }
        Some(ResidentProcessKind::Repl { slug }) => {
            (holder_job, None, Some(format!("repl/{slug}")))
        }
        Some(ResidentProcessKind::DevInstance { .. }) => {
            let job = match (holder_job, &address.project_id, &address.selector) {
                (Some(job), ..) => Some(job),
                (None, Some(project), Some(branch)) => {
                    branch_owner_job(orch, project, branch).await?
                }
                _ => None,
            };
            (job, None, None)
        }
        _ => (holder_job, None, None),
    };
    let uri = match (&job_id, path) {
        (Some(job), Some(path)) => crate::jobs::queries::home_uri_for_job(&orch.db.local, job)
            .await
            .ok()
            .flatten()
            .map(|home| format!("{home}/{path}")),
        _ => None,
    };
    Ok(ResidentOwner {
        job_id,
        session_id,
        uri,
    })
}

/// `terminal `lab``, `REPL `py``, `dev instance`: the resident as its owner
/// knows it.
fn resident_noun(kind: Option<&ResidentProcessKind>) -> String {
    match kind {
        Some(ResidentProcessKind::Terminal { slug }) => format!("terminal `{slug}`"),
        Some(ResidentProcessKind::Repl { slug }) => format!("REPL `{slug}`"),
        Some(ResidentProcessKind::DevInstance { .. }) => "dev instance".into(),
        _ => "resident process".into(),
    }
}

/// The message for the owner and, for a terminal, the line for its own
/// transcript.
///
/// What happens to a terminal after the restart is measured, not assumed:
/// the directive ends the executor's link before it stops its residents, so
/// no exit reaches the runner, and the returning executor reports the process
/// under a new cell epoch, which the runner settles as lost. Cairn does not
/// start such a terminal again, and the text says so. Whether a REPL or a dev
/// instance is started again belongs to its own lifecycle, so their text
/// claims neither.
fn notice_text(notice: &OwnerNotice, uri: Option<&str>) -> (String, Option<String>) {
    let kind = notice.resident.address.kind.as_ref();
    let noun = resident_noun(kind);
    let terminal = matches!(kind, Some(ResidentProcessKind::Terminal { .. }));
    let machine = &notice.name;
    let after = if terminal {
        format!(
            "Cairn does not start it again: restart it once {machine} is back if you still need it"
        )
    } else {
        format!("start it again once {machine} is back if you still need it")
    };
    let link = uri.map(|uri| format!(" ({uri})")).unwrap_or_default();
    match notice.kind {
        NoticeKind::Announced {
            restart_no_earlier_than_unix_ms,
            commands_in_flight,
        } => {
            let commands = match commands_in_flight {
                0 => String::new(),
                1 => ", once the command running there finishes".to_string(),
                count => format!(", once the {count} commands running there finish"),
            };
            let at = describe_clock(restart_no_earlier_than_unix_ms);
            let options = if terminal {
                " Before then you can checkpoint or hand off its work, or stop it yourself (delete the terminal) so the restart does not cut it mid-step."
            } else {
                ""
            };
            (
                format!(
                    "{machine}, where your {noun}{link} runs, restarts to install executor build {} no earlier than {at}{commands}. The restart ends your {noun}; {after}.{options}",
                    notice.build_id
                ),
                terminal.then(|| {
                    format!(
                        "\r\n[Cairn] {machine} restarts for an executor update no earlier than {at}{commands}. This terminal ends then and is not started again.\r\n"
                    )
                }),
            )
        }
        NoticeKind::Ending => (
            format!(
                "{machine} is restarting now to install executor build {}, and your {noun}{link} ends with it; {after}.",
                notice.build_id
            ),
            terminal.then(|| {
                format!(
                    "\r\n[Cairn] {machine} is restarting now for an executor update. This terminal ends with it and is not started again.\r\n"
                )
            }),
        ),
    }
}

/// What a `waitFor` parked on a terminal answers when its machine's restart is
/// announced or begins: the notice itself, as a sentence the agent reads as
/// news rather than as its wait failing.
fn terminal_wait_cycling_result(notice: &OwnerNotice, slug: &str, message: &str) -> String {
    match notice.kind {
        NoticeKind::Announced {
            restart_no_earlier_than_unix_ms,
            ..
        } => format!(
            "Your wait on terminal `{slug}` returned early so you can act before its machine restarts: {message} The terminal is still running until then, and you can wait on it again with the same waitFor. (outcome: terminal_cycling; restartNoEarlierThan: {})",
            describe_clock(restart_no_earlier_than_unix_ms)
        ),
        NoticeKind::Ending => format!(
            "Your wait on terminal `{slug}` returned because its machine is restarting now: {message} (outcome: terminal_restarting_now)"
        ),
    }
}

/// Tell one resident's owner what the update means for it: into the
/// terminal's own transcript when it is live, by ending a `waitFor` the owner
/// is parked on for that terminal (the wait answers with the notice), and as
/// one message to the owner's latest run, idempotent per turn, resident and
/// notice, so a pass that repeats a notice never tells the owner twice. The
/// message is a `steer`: an owner mid-turn reads it at its next tool boundary,
/// and an idle one wakes for it, because the window it describes is short.
pub(crate) async fn deliver_owner_notice(
    orch: &Orchestrator,
    notice: &OwnerNotice,
    now_unix_ms: u64,
) -> ResidentNotice {
    let not_yet = |error: String| {
        log::warn!(
            "self-update: could not look up the owner of {} on {}; retried next pass: {error}",
            notice.resident.label,
            notice.name
        );
        ResidentNotice::Pending
    };
    let owner = match resolve_owner(orch, &notice.resident).await {
        Ok(owner) => owner,
        Err(error) => return not_yet(error),
    };
    let (message, transcript) = notice_text(notice, owner.uri.as_deref());
    if let (Some(session), Some(line)) = (&owner.session_id, &transcript) {
        crate::mcp::handlers::terminal::announce_in_terminal(orch, session, line);
    }
    let run = match &owner.job_id {
        Some(job) => {
            match crate::messages::delivery::latest_run_for_job_async(&orch.db.local, job).await {
                Ok(run) => run,
                Err(error) => return not_yet(error),
            }
        }
        None => None,
    };
    let Some(run) = run else {
        log::info!(
            "self-update: no owner found for {} on {}; noted only in its own transcript where it has one",
            notice.resident.label,
            notice.name
        );
        return ResidentNotice::OwnerNotFound;
    };
    if let (Some(ResidentProcessKind::Terminal { slug }), Some(job_id)) = (
        notice.resident.address.kind.as_ref(),
        owner.job_id.as_deref(),
    ) {
        let result = terminal_wait_cycling_result(notice, slug, &message);
        let terminal = crate::mcp::handlers::durable_suspend::CyclingTerminal {
            uri: owner.uri.as_deref(),
            job_id,
            slug,
        };
        match crate::mcp::handlers::durable_suspend::supersede_terminal_waits(
            orch,
            &orch.db.local,
            job_id,
            &terminal,
            &result,
        )
        .await
        {
            Ok(ended) if !ended.is_empty() => log::info!(
                "self-update: ended {}'s parked {} so it reads the notice for {} now",
                job_id,
                ended.join(", "),
                notice.resident.label
            ),
            Ok(_) => {}
            Err(error) => log::warn!(
                "self-update: could not end the waits parked on {}; the notice is still queued: {error}",
                notice.resident.label
            ),
        }
    }
    let key = format!(
        "self-update:{}:{}:{}",
        notice.operation_id,
        notice.resident.key,
        match notice.kind {
            NoticeKind::Announced { .. } => "announced",
            NoticeKind::Ending => "ending",
        }
    );
    let queue_orch = orch.clone();
    let queued = tokio::task::spawn_blocking(move || {
        crate::messages::delivery::queue_system_direct_once(
            &queue_orch,
            &run,
            &message,
            DeliveryUrgency::Steer,
            &key,
        )
    })
    .await
    .map_err(|error| error.to_string())
    .and_then(|queued| queued);
    match queued {
        Ok(()) => ResidentNotice::Told {
            at_unix_ms: now_unix_ms,
        },
        Err(error) => {
            log::warn!(
                "self-update: could not tell the owner of {} on {}: {error}",
                notice.resident.label,
                notice.name
            );
            ResidentNotice::Pending
        }
    }
}

/// Commit the holds one pass requires. Runs under the pass gate, before the
/// schedule applies or any directive is sent. When the committed projection
/// already holds exactly the remembered tokens, the write would change
/// nothing, so the pass touches neither the settings file nor the projection.
fn persist_required(
    orch: &Orchestrator,
    required: &[SelfUpdateDrain],
    now_unix_ms: u64,
) -> Result<(), String> {
    let runtime = &orch.fleet.self_update_runtime;
    let principal = PrincipalRef::Machine {
        device_id: orch.anon_device_manager.device_id().to_string(),
    };
    let tokens = runtime.tokens.lock().unwrap().clone();
    if orch.fleet.with_committed_drain_records(|records| {
        holds_exact(records, required, &runtime.boot_id, &principal, &tokens)
    }) {
        return Ok(());
    }
    let next = management::mutate_fleet(orch, |config| {
        apply_required_drains(
            config,
            required,
            &runtime.boot_id,
            &principal,
            now_unix_ms
                .try_into()
                .map_err(|_| "self-update timestamp overflow")?,
            &tokens,
        )
    })?;
    *runtime.tokens.lock().unwrap() = next;
    Ok(())
}

fn self_update_context(hold: &SelfUpdateDrain, boot_id: &str) -> RuntimeDrainContext {
    RuntimeDrainContext {
        kind: RuntimeDrainKind::SelfUpdate,
        operation_id: hold.operation_id.clone(),
        turn_id: hold.operation_id.clone(),
        runner_boot_id: boot_id.to_string(),
        admission_effect: AdmissionEffect::RefuseNewEnvironments,
    }
}

fn self_update_reason(hold: &SelfUpdateDrain) -> String {
    format!(
        "self-update to {}; completion estimate unknown",
        hold.build_id
    )
}

/// Whether [`apply_required_drains`] would leave `records` untouched and
/// return `tokens` again: one remembered token per required hold, each naming
/// a committed record of the same owner, generation, reason and principal
/// (the impose no-op condition), and no other self-update record of this boot
/// standing anywhere. Anything else takes the full serialized write.
fn holds_exact(
    records: &HashMap<String, Vec<cairn_config::drain::DrainRecord>>,
    required: &[SelfUpdateDrain],
    boot_id: &str,
    principal: &PrincipalRef,
    tokens: &HashMap<String, RuntimeDrainToken>,
) -> bool {
    if tokens.len() != required.len() {
        return false;
    }
    let held = required.iter().all(|hold| {
        let Some(token) = tokens.get(&hold.executor_id) else {
            return false;
        };
        let context = self_update_context(hold, boot_id);
        token.context == context
            && token.drain_key == FleetConfig::drain_key(&hold.executor_id)
            && records.get(&hold.executor_id).is_some_and(|records| {
                records.iter().any(|record| {
                    record.owner
                        == DrainOwner::Runtime {
                            context: context.clone(),
                        }
                        && record.generation == token.generation
                        && record.reason == self_update_reason(hold)
                        && record.imposed_by() == Some(principal)
                })
            })
    });
    held && records.iter().all(|(executor_id, records)| {
        records.iter().all(|record| match &record.owner {
            DrainOwner::Runtime { context }
                if context.kind == RuntimeDrainKind::SelfUpdate
                    && context.runner_boot_id == boot_id =>
            {
                tokens.get(executor_id).is_some_and(|token| {
                    token.context == *context && token.generation == record.generation
                })
            }
            _ => true,
        })
    })
}

/// A new runner has no applied rollout turns. Retire only exact prior-boot
/// self-update tokens before resuming; other runtime kinds are not ours.
pub(super) fn recover_orphans(
    config: &mut FleetConfig,
    fleet: &super::Fleet,
) -> Result<(), String> {
    for (executor_id, records) in config.drain_records_executor_ids() {
        for record in records {
            let DrainOwner::Runtime { context } = record.owner else {
                continue;
            };
            if context.kind == RuntimeDrainKind::SelfUpdate
                && context.runner_boot_id != fleet.self_update_runtime.boot_id
            {
                config.drains.release_runtime(&RuntimeDrainToken {
                    drain_key: FleetConfig::drain_key(&executor_id).to_string(),
                    executor_id: executor_id.clone(),
                    context,
                    generation: record.generation,
                })?;
            }
        }
    }
    Ok(())
}

fn apply_required_drains(
    config: &mut FleetConfig,
    required: &[SelfUpdateDrain],
    boot_id: &str,
    principal: &PrincipalRef,
    now_unix_ms: i64,
    tokens: &HashMap<String, RuntimeDrainToken>,
) -> Result<HashMap<String, RuntimeDrainToken>, String> {
    // Compare every remembered token before making any change. In particular,
    // replacement of this runtime's own record is not permission to clear it.
    for token in tokens.values() {
        let current = config
            .drains
            .records(&token.executor_id)
            .into_iter()
            .find(|record| {
                record.owner
                    == DrainOwner::Runtime {
                        context: token.context.clone(),
                    }
            });
        if current.is_some_and(|record| record.generation != token.generation) {
            return Err(format!(
                "self-update drain token for {} was replaced; no rollout effect applied",
                token.executor_id
            ));
        }
    }
    for token in tokens.values() {
        if !required.iter().any(|hold| {
            hold.executor_id == token.executor_id && hold.operation_id == token.context.operation_id
        }) {
            config.drains.release_runtime(token)?;
        }
    }
    let mut next = HashMap::new();
    for hold in required {
        let token = config.drains.impose_runtime(
            &hold.executor_id,
            self_update_context(hold, boot_id),
            principal,
            &self_update_reason(hold),
            now_unix_ms,
        )?;
        next.insert(hold.executor_id.clone(), token);
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notice_for(kind: NoticeKind, resident_kind: ResidentProcessKind) -> OwnerNotice {
        OwnerNotice {
            executor_id: "id-fedora".into(),
            name: "fedora".into(),
            build_id: "build-new".into(),
            operation_id: "turn-1".into(),
            resident: Resident {
                key: "process:cell-1/terminal:lab/1".into(),
                label: "terminal `lab` of proj/1".into(),
                address: super::super::rollout::ResidentAddress {
                    process_key: "terminal:lab".into(),
                    kind: Some(resident_kind),
                    holder: Some(ResidencyHolder::Job {
                        job_id: "job-thread".into(),
                    }),
                    owner: None,
                    project_id: None,
                    selector: None,
                },
            },
            kind,
        }
    }

    /// The owner is told the restart's time and that the terminal ends and is
    /// not started again, which is what a self-update restart measurably does
    /// to it; a REPL's text claims nothing about its restart.
    #[test]
    fn the_notice_names_the_machine_the_time_and_the_outcome() {
        let at = 1_800_000_000_000 + 5 * 60_000;
        let terminal = ResidentProcessKind::Terminal { slug: "lab".into() };
        let (message, transcript) = notice_text(
            &notice_for(
                NoticeKind::Announced {
                    restart_no_earlier_than_unix_ms: at,
                    commands_in_flight: 2,
                },
                terminal.clone(),
            ),
            Some("cairn://p/proj/1/1/builder/terminal/lab"),
        );
        assert_eq!(
            message,
            format!(
                "fedora, where your terminal `lab` (cairn://p/proj/1/1/builder/terminal/lab) runs, restarts to install executor build build-new no earlier than {}, once the 2 commands running there finish. The restart ends your terminal `lab`; Cairn does not start it again: restart it once fedora is back if you still need it. Before then you can checkpoint or hand off its work, or stop it yourself (delete the terminal) so the restart does not cut it mid-step.",
                describe_clock(at)
            )
        );
        assert!(transcript
            .unwrap()
            .contains("This terminal ends then and is not started again."));

        let (message, transcript) = notice_text(&notice_for(NoticeKind::Ending, terminal), None);
        assert_eq!(
            message,
            "fedora is restarting now to install executor build build-new, and your terminal `lab` ends with it; Cairn does not start it again: restart it once fedora is back if you still need it."
        );
        assert!(transcript.is_some());

        let (message, transcript) = notice_text(
            &notice_for(
                NoticeKind::Ending,
                ResidentProcessKind::Repl { slug: "py".into() },
            ),
            None,
        );
        assert!(
            message.ends_with("start it again once fedora is back if you still need it."),
            "{message}"
        );
        assert!(!message.contains("does not start"), "{message}");
        assert!(transcript.is_none(), "a REPL has no terminal transcript");
    }

    /// An owner and its terminal `lab`, with nothing parked yet.
    async fn owner_with_a_terminal(
        name: &str,
    ) -> (Orchestrator, std::sync::Arc<crate::storage::LocalDb>) {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj-1','default','Test Project','proj','/tmp/test-repo',1,1);
             INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('thread-1','proj-1','general','active','none',1,1);
             INSERT INTO jobs (id, thread_id, project_id, node_name, uri_segment, status, created_at, updated_at)
               VALUES ('job-thread','thread-1','proj-1','thread','thread','idle',1,1);
             INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
               VALUES ('run-1','proj-1','job-thread','live',1,1);
             INSERT INTO sessions (id, job_id, backend, status, created_at, updated_at)
               VALUES ('sess-1','job-thread','claude','open',1,1);
             INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
               VALUES ('turn-1','sess-1','run-1','job-thread',1,'yielded',1,1);
             INSERT INTO job_terminals (id, job_id, session_id, command, status, created_at, slug, residency_holder)
               VALUES ('t-1','job-thread','terminal:lab','./prove','running',1,'lab','job:job-thread');",
        )
        .await
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp,
        )
        .build();
        (orch, db)
    }

    /// Park the owner on `waitFor: terminal lab output` (or, with `batch`, a
    /// run batch beside it) as wait row `id`.
    async fn park(db: &crate::storage::LocalDb, id: &str, batch: Option<&str>) {
        use crate::mcp::handlers::durable_suspend::Condition;
        let condition = Condition::Terminal {
            uri: "cairn://p/proj/thread/terminal/lab".into(),
            slug: "lab".into(),
            owner: Some(crate::mcp::handlers::terminal_ref::TerminalOwner::Job {
                job_id: "job-thread".into(),
            }),
            on: crate::mcp::handlers::run::TerminalWaitEvent::Output,
            phrase: Some("PROVED".into()),
        };
        let insert = |id: String, json: String, kind: &'static str| async move {
            db.execute(
                "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,kind,state,created_at)
                 VALUES(?1,'job-thread','run-1','sess-1','turn-1',?1,?2,?3,'pending',1)",
                cairn_db::turso::params![id, json, kind],
            )
            .await
            .unwrap();
        };
        insert(
            id.to_string(),
            serde_json::to_string(&condition).unwrap(),
            condition.kind(),
        )
        .await;
        if let Some(batch) = batch {
            insert(
                batch.to_string(),
                r#"{"kind":"run_batch","request_id":"req","commits":false,"label":"cargo test"}"#
                    .into(),
                "run_batch",
            )
            .await;
        }
    }

    async fn wait_row(db: &crate::storage::LocalDb, id: &str) -> (String, Option<String>) {
        db.query_one(
            "SELECT state, resolution_json FROM agent_waits WHERE id = ?1",
            cairn_db::turso::params![id.to_string()],
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap()
    }

    fn announced_at(at: u64) -> OwnerNotice {
        notice_for(
            NoticeKind::Announced {
                restart_no_earlier_than_unix_ms: at,
                commands_in_flight: 0,
            },
            ResidentProcessKind::Terminal { slug: "lab".into() },
        )
    }

    /// cairn/7082 required 4. An owner parked on a terminal's output cannot
    /// read a message, so the 5-minute notice ends the wait itself: the wait
    /// answers with the notice, the restart time and what the owner can do,
    /// before any restart is directed. A later Ending notice leaves that
    /// answered wait alone.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cycling_notice_ends_the_owners_parked_terminal_wait_with_the_notice() {
        let (orch, db) = owner_with_a_terminal("self-update-ends-wait").await;
        park(&db, "wait-1", None).await;
        let at = 1_800_000_000_000 + 5 * 60_000;

        assert_eq!(
            deliver_owner_notice(&orch, &announced_at(at), 7).await,
            ResidentNotice::Told { at_unix_ms: 7 }
        );
        let (state, answer) = wait_row(&db, "wait-1").await;
        assert_ne!(state, "pending", "the wait is ended, not left parked");
        let answer = answer.unwrap();
        assert!(
            answer.starts_with("Your wait on terminal `lab` returned early"),
            "{answer}"
        );
        assert!(
            answer.contains(&describe_clock(at)),
            "names the restart time: {answer}"
        );
        assert!(
            answer.contains(
                "checkpoint or hand off its work, or stop it yourself (delete the terminal)"
            ),
            "names what the owner can do: {answer}"
        );
        assert!(answer.contains("outcome: terminal_cycling"), "{answer}");
        for refusal in ["interrupt", "cancel", "declin", "reject", "denied"] {
            assert!(!answer.to_ascii_lowercase().contains(refusal), "{answer}");
        }

        let ending = notice_for(
            NoticeKind::Ending,
            ResidentProcessKind::Terminal { slug: "lab".into() },
        );
        deliver_owner_notice(&orch, &ending, 8).await;
        assert_eq!(
            wait_row(&db, "wait-1").await.1.as_deref(),
            Some(answer.as_str()),
            "an answered wait is never answered again"
        );

        let urgency: Option<String> = db
            .query_one(
                "SELECT urgency FROM messages WHERE id = ?1",
                cairn_db::turso::params![crate::messages::delivery::system_direct_once_message_id(
                    &format!(
                        "self-update:turn-1:{}:announced",
                        announced_at(at).resident.key
                    )
                )],
                |row| row.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(urgency.as_deref(), Some("steer"));
    }

    /// A wait armed after the 5-minute notice is ended by the Ending one.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_ending_notice_ends_a_wait_armed_after_the_announcement() {
        let (orch, db) = owner_with_a_terminal("self-update-ending-wait").await;
        deliver_owner_notice(&orch, &announced_at(1_800_000_300_000), 7).await;
        park(&db, "wait-late", None).await;

        let ending = notice_for(
            NoticeKind::Ending,
            ResidentProcessKind::Terminal { slug: "lab".into() },
        );
        deliver_owner_notice(&orch, &ending, 8).await;
        let (state, answer) = wait_row(&db, "wait-late").await;
        assert_ne!(state, "pending");
        let answer = answer.unwrap();
        assert!(answer.contains("is restarting now"), "{answer}");
        assert!(
            answer.contains("outcome: terminal_restarting_now"),
            "{answer}"
        );
    }

    /// A turn with a run batch in flight is never resumed early: the notice
    /// reaches it as a message, and its waits stay parked.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cycling_notice_never_resumes_a_turn_with_work_in_flight() {
        let (orch, db) = owner_with_a_terminal("self-update-work-in-flight").await;
        park(&db, "wait-1", Some("batch-1")).await;

        assert_eq!(
            deliver_owner_notice(&orch, &announced_at(1_800_000_300_000), 7).await,
            ResidentNotice::Told { at_unix_ms: 7 }
        );
        assert_eq!(wait_row(&db, "wait-1").await.0, "pending");
        assert_eq!(wait_row(&db, "batch-1").await.0, "pending");
    }

    /// One notice reaches the owner's run once however many passes repeat it,
    /// the two notices of a turn are two messages, and a resident nobody
    /// owns is reported as such without failing the pass.
    #[tokio::test]
    async fn an_owner_is_told_each_notice_once() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(crate::storage::migrated_test_db("self-update-notices").await);
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj-1','default','Test Project','proj','/tmp/test-repo',1,1);
             INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('thread-1','proj-1','general','active','none',1,1);
             INSERT INTO jobs (id, thread_id, project_id, node_name, uri_segment, status, created_at, updated_at)
               VALUES ('job-thread','thread-1','proj-1','thread','thread','idle',1,1);
             INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
               VALUES ('run-1','proj-1','job-thread','live',1,1);
             INSERT INTO job_terminals (id, job_id, session_id, command, title, status, created_at, slug, residency_holder)
               VALUES ('t-1','job-thread','terminal:lab','','Shell','running',1,'lab','job:job-thread');",
        )
        .await
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().to_path_buf(),
        )
        .build();
        let terminal = ResidentProcessKind::Terminal { slug: "lab".into() };
        let announced = notice_for(
            NoticeKind::Announced {
                restart_no_earlier_than_unix_ms: 1,
                commands_in_flight: 0,
            },
            terminal.clone(),
        );
        let ending = notice_for(NoticeKind::Ending, terminal.clone());

        for _ in 0..2 {
            assert_eq!(
                deliver_owner_notice(&orch, &announced, 7).await,
                ResidentNotice::Told { at_unix_ms: 7 }
            );
        }
        assert_eq!(
            deliver_owner_notice(&orch, &ending, 8).await,
            ResidentNotice::Told { at_unix_ms: 8 }
        );
        let delivered = |key: &str| {
            let id = crate::messages::delivery::system_direct_once_message_id(key);
            let db = db.clone();
            async move {
                db.query_one(
                    "SELECT COUNT(*) FROM message_recipients WHERE message_id = ?1 AND recipient_run_id = 'run-1'",
                    cairn_db::turso::params![id.as_str()],
                    |row| row.i64(0),
                )
                .await
                .unwrap()
            }
        };
        assert_eq!(
            delivered("self-update:turn-1:process:cell-1/terminal:lab/1:announced").await,
            1
        );
        assert_eq!(
            delivered("self-update:turn-1:process:cell-1/terminal:lab/1:ending").await,
            1
        );

        // A dev instance as production records it: its own holder, and an
        // owner reference that never names a job. Its owner is the job whose
        // branch it was built from.
        db.execute_script(
            "UPDATE jobs SET branch = 'agent/proj-1-builder-0' WHERE id = 'job-thread';",
        )
        .await
        .unwrap();
        let mut dev = notice_for(
            NoticeKind::Ending,
            ResidentProcessKind::DevInstance {
                source_terminal_session_id: None,
            },
        );
        dev.resident.key = "process:cell-2/dev/1".into();
        dev.resident.address.process_key = "dev".into();
        dev.resident.address.holder = Some(ResidencyHolder::DevInstance {
            instance_id: "inst-1".into(),
        });
        dev.resident.address.owner = Some(cairn_common::executor_protocol::CellOwnerRef {
            project_id: "proj-1".into(),
            project_key: Some("proj".into()),
            issue_number: Some(1),
            job_id: None,
            execution_seq: None,
            node_kind: Some("builder".into()),
        });
        dev.resident.address.project_id = Some("proj-1".into());
        dev.resident.address.selector = Some("agent/proj-1-builder-0".into());
        assert_eq!(
            deliver_owner_notice(&orch, &dev, 10).await,
            ResidentNotice::Told { at_unix_ms: 10 }
        );
        assert_eq!(
            delivered("self-update:turn-1:process:cell-2/dev/1:ending").await,
            1
        );

        let mut orphan = notice_for(NoticeKind::Ending, terminal);
        orphan.resident.address.holder = Some(ResidencyHolder::ProjectTerminals {
            project_id: "proj-1".into(),
        });
        orphan.resident.address.process_key = "terminal:nobody".into();
        assert_eq!(
            deliver_owner_notice(&orch, &orphan, 9).await,
            ResidentNotice::OwnerNotFound
        );
    }

    #[tokio::test]
    async fn rollout_cannot_open_an_uninitialized_fleet_projection() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::LocalDb;
        use crate::storage::SearchIndex;
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open_migrated_for_test(temp.path().join("startup.db"))
                .await
                .unwrap(),
        );
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db, search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().to_path_buf(),
        )
        .build();
        // Fixtures otherwise start initialized; emulate the untouched startup
        // gate, before any load failure has supplied an error diagnostic.
        {
            let mut projection = orch.fleet.drain_projection.lock().unwrap();
            projection.initialized = false;
            projection.initialization_error = None;
        }
        assert!(orch.fleet.drain_initialization_error().is_none());
        assert!(advance(&orch, 1_700_000_000_000).is_err());
        assert!(!orch.fleet.drain_projection_initialized());
        assert!(!temp.path().join("settings.yaml").exists());
    }

    /// A stable pass reads the committed projection and stops: the settings
    /// file is not read (a corrupt one is not noticed) and nothing republishes.
    /// A changed requirement, a lost record or an orphan takes the full write.
    #[tokio::test]
    async fn a_stable_pass_writes_and_publishes_nothing() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::LocalDb;
        use crate::storage::SearchIndex;
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open_migrated_for_test(temp.path().join("stable.db"))
                .await
                .unwrap(),
        );
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db, search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().to_path_buf(),
        )
        .build();
        let settings = temp.path().join("settings.yaml");
        let revision = || {
            orch.fleet
                .drain_projection
                .lock()
                .unwrap()
                .view("remote-id")
                .machine_revision
        };

        persist_required(&orch, &desired("build-a"), 1).unwrap();
        let first = revision();
        assert_eq!(orch.fleet.owned_drain_records("remote-id").len(), 1);
        let committed = std::fs::read_to_string(&settings).unwrap();

        // Prove the stable pass never opens the file: it would refuse this.
        std::fs::write(&settings, "buildSlots: [not, a, fleet").unwrap();
        persist_required(&orch, &desired("build-a"), 2).unwrap();
        persist_required(&orch, &desired("build-a"), 3).unwrap();
        assert_eq!(
            std::fs::read_to_string(&settings).unwrap(),
            "buildSlots: [not, a, fleet"
        );
        assert_eq!(revision(), first);
        // A changed requirement is a real write, so it now meets the file.
        assert!(persist_required(&orch, &desired("build-b"), 4).is_err());

        std::fs::write(&settings, committed).unwrap();
        persist_required(&orch, &desired("build-b"), 5).unwrap();
        assert!(revision() > first);
        let records = orch.fleet.owned_drain_records("remote-id");
        assert_eq!(records.len(), 1);
        assert!(records[0].reason.contains("build-b"));

        persist_required(&orch, &[], 6).unwrap();
        assert!(orch.fleet.owned_drain_records("remote-id").is_empty());
        assert!(orch
            .fleet
            .self_update_runtime
            .tokens
            .lock()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn exactness_requires_every_token_committed_and_no_orphan() {
        let principal = principal();
        let mut config = FleetConfig::default();
        let tokens = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            1,
            &HashMap::new(),
        )
        .unwrap();
        let records = |config: &FleetConfig| -> HashMap<_, _> {
            config.drain_records_executor_ids().into_iter().collect()
        };
        let exact = |config: &FleetConfig, required: &[SelfUpdateDrain], tokens: &HashMap<_, _>| {
            holds_exact(&records(config), required, "boot", &principal, tokens)
        };
        assert!(exact(&config, &desired("build-a"), &tokens));
        assert!(!exact(&config, &desired("build-b"), &tokens));
        assert!(!exact(&config, &[], &tokens));
        assert!(!exact(&config, &desired("build-a"), &HashMap::new()));
        assert!(!holds_exact(
            &records(&config),
            &desired("build-a"),
            "boot",
            &PrincipalRef::Machine {
                device_id: "other".into()
            },
            &tokens
        ));
        // Nothing required and nothing remembered, but this boot's hold stands.
        assert!(!exact(&config, &[], &HashMap::new()));

        let mut orphaned = config.clone();
        let mut context = tokens["remote-id"].context.clone();
        context.operation_id = "stale-turn".into();
        context.turn_id = "stale-turn".into();
        orphaned
            .drains
            .impose_runtime("other-id", context, &principal, "stale", 2)
            .unwrap();
        assert!(!exact(&orphaned, &desired("build-a"), &tokens));

        let mut lost = config.clone();
        lost.drains.release_runtime(&tokens["remote-id"]).unwrap();
        assert!(!exact(&lost, &desired("build-a"), &tokens));
        // Other owners and other boots' records are not this pass's concern.
        let mut shared = config.clone();
        shared
            .drains
            .impose_thread("remote-id", "thread-a", &principal, "maintenance", 3)
            .unwrap();
        apply_required_drains(
            &mut shared,
            &desired("build-a"),
            "prior-boot",
            &principal,
            4,
            &HashMap::new(),
        )
        .unwrap();
        assert!(exact(&shared, &desired("build-a"), &tokens));
    }

    fn principal() -> PrincipalRef {
        PrincipalRef::Machine {
            device_id: "runner".into(),
        }
    }

    fn desired(build: &str) -> Vec<SelfUpdateDrain> {
        vec![SelfUpdateDrain {
            executor_id: "remote-id".into(),
            operation_id: "turn".into(),
            build_id: build.into(),
        }]
    }

    #[test]
    fn retries_are_stable_and_release_preserves_other_owners() {
        let mut config = FleetConfig::default();
        let principal = principal();
        config
            .drains
            .impose_thread("remote-id", "thread-a", &principal, "maintenance", 1)
            .unwrap();
        let tokens = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            2,
            &HashMap::new(),
        )
        .unwrap();
        let revision = config.drains.machine_revision("remote-id");
        let retry = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            3,
            &tokens,
        )
        .unwrap();
        assert_eq!(tokens, retry);
        assert_eq!(config.drains.machine_revision("remote-id"), revision);
        let retargeted = apply_required_drains(
            &mut config,
            &desired("build-b"),
            "boot",
            &principal,
            4,
            &tokens,
        )
        .unwrap();
        assert!(retargeted["remote-id"].generation > tokens["remote-id"].generation);
        assert!(!config.drains.release_runtime(&tokens["remote-id"]).unwrap());
        apply_required_drains(&mut config, &[], "boot", &principal, 5, &retargeted).unwrap();
        let remaining = config.drains.records("remote-id");
        assert_eq!(remaining.len(), 1);
        assert!(
            matches!(&remaining[0].owner, DrainOwner::Thread { thread_id } if thread_id == "thread-a")
        );
    }

    #[test]
    fn a_replaced_runtime_record_is_neither_released_nor_overwritten() {
        let mut config = FleetConfig::default();
        let principal = principal();
        let tokens = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            1,
            &HashMap::new(),
        )
        .unwrap();
        let replacement = config
            .drains
            .impose_runtime(
                "remote-id",
                tokens["remote-id"].context.clone(),
                &principal,
                "replacement reason",
                2,
            )
            .unwrap();
        let before = config.drains.clone();
        assert!(apply_required_drains(&mut config, &[], "boot", &principal, 3, &tokens).is_err());
        assert_eq!(config.drains, before);
        assert!(apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            4,
            &tokens
        )
        .is_err());
        assert_eq!(
            config.drains.records("remote-id")[0].generation,
            replacement.generation
        );
    }

    #[test]
    fn startup_recovers_only_prior_boot_self_update_tokens() {
        let mut config = FleetConfig::default();
        let principal = principal();
        let fleet = super::super::Fleet::default();
        let old = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "prior-boot",
            &principal,
            1,
            &HashMap::new(),
        )
        .unwrap();
        let mut quiesce_context = old["remote-id"].context.clone();
        quiesce_context.kind = RuntimeDrainKind::Quiesce;
        let quiesce = config
            .drains
            .impose_runtime("remote-id", quiesce_context, &principal, "quiesce", 2)
            .unwrap();
        config
            .drains
            .impose_operator("remote-id", &principal, "operator maintenance", 3)
            .unwrap();
        let mut current = old["remote-id"].context.clone();
        current.runner_boot_id = fleet.self_update_runtime.boot_id.clone();
        current.operation_id = "current-turn".into();
        current.turn_id = "current-turn".into();
        let current = config
            .drains
            .impose_runtime("remote-id", current, &principal, "current self-update", 4)
            .unwrap();
        recover_orphans(&mut config, &fleet).unwrap();
        let remaining = config.drains.records("remote-id");
        assert_eq!(remaining.len(), 3);
        assert!(!remaining
            .iter()
            .any(|record| record.generation == old["remote-id"].generation));
        assert!(remaining
            .iter()
            .any(|record| record.generation == quiesce.generation));
        assert!(remaining
            .iter()
            .any(|record| record.generation == current.generation));
        let revision = config.drains.machine_revision("remote-id");
        recover_orphans(&mut config, &fleet).unwrap();
        assert_eq!(config.drains.machine_revision("remote-id"), revision);
    }
}
