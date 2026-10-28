//! Claude CLI backend implementation.
//!
//! Resolves launch configuration and credentials, then hosts the session-owned
//! stream-json reader and applies application lifecycle policy to its reports.

use crate::agent_process::completion::ClaudeTurnOutcome;
#[cfg(test)]
use crate::agent_process::stream::TranscriptEvent;
use crate::orchestrator::lifecycle::{
    apply_claude_artifact_handoff, apply_claude_turn_completion, was_host_interrupted,
    ClaudeTurnContext,
};
use cairn_models::interface::session::{
    ClaudeMode, ClaudeSpec, ProviderSpec, SecretEnv, SessionError, SessionEvents, SessionSpec,
};
use cairn_models::wire::claude_stream::{ProviderDepletion, RateLimitInfo, StreamEvidence};

use super::run_state::{
    is_task_spawned_run, record_run_account, resolve_run_db, run_backend_db, run_job_id,
    run_status, transition_run_to_live,
};

use crate::credentials::{AccountBlock, RoutedProvider};
use crate::models::{ContextTokenState, RunStatus, TurnEndReason};
use crate::orchestrator::session::{
    assemble_prompt_segments, flatten_prompt_segments, get_claude_path, insert_error_event,
    persist_system_prompt_event, select_routed_serving_identity, system_prompt_placement,
    SelectionMode,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_models::types::catalog::{
    OptionChoice, OptionKind, ProviderOptionDescriptor, ProviderOptionKey,
};
use cairn_models::types::usage::ProviderUsageSnapshot;
use std::collections::BTreeMap;
use std::io::BufRead;
use std::sync::{Arc, Mutex};
use std::thread;

use super::{
    AgentBackend, BackendFailure, CompletionError, CompletionFailure, CompletionOutcome,
    CompletionRequest, ResolvedTools, SessionConfig,
};
use cairn_models::types::catalog::DiscoveredModel;

use cairn_session::backends::claude::*;
pub use cairn_session::backends::claude::{
    claude_call_concurrency_ceiling, dispatch_claude_result, ClaudeResultContext,
    CLAUDE_CALL_MAX_CONCURRENCY,
};

/// Refusal shown only when no Claude credential is configured.
pub(crate) const NO_CLAUDE_CREDENTIAL: &str = "No Claude account is configured. Sign in to a Claude account, or add an Anthropic API key, in Settings → Providers.";

/// Settle a launch's credential as the process environment presents it,
/// failing closed.
///
/// A session that spawns without an explicit credential runs on whatever
/// account is signed in at the user level: invisible to usage routing,
/// unattributable, and impossible to switch away from when it runs out.
///
/// The CLI reads an API key from its environment, so the plaintext is
/// unavoidable; what the broker adds is that the value is a registered scrub
/// target before it is injected, and that the injection appears in the lease
/// inventory. A managed account's presentation comes from the installed
/// subscription support, which is what refuses a managed account in a build
/// without it.
///
/// A managed account's credential is an access token with enough life left
/// that the process never refreshes; the generation it was is returned beside
/// it so a live process can be pushed the next one.
pub(crate) fn claude_launch_credential(
    orch: &Orchestrator,
    user: &crate::identity::UserIdentity,
) -> Result<(SecretEnv, Option<u64>), String> {
    const ANTHROPIC_PROVIDER: &str = "anthropic";
    match &user.claude_auth {
        Some(crate::credentials::ClaudeAuth::ApiKey(key)) => {
            let leased = crate::security::broker::backend::agent_credential(
                ANTHROPIC_PROVIDER,
                user.email.as_str(),
                crate::security::broker::backend::CLAUDE_ROLE,
                key,
            )?;
            log::info!("Setting ANTHROPIC_API_KEY (len={})", leased.len());
            Ok((SecretEnv::new().with("ANTHROPIC_API_KEY", leased), None))
        }
        Some(crate::credentials::ClaudeAuth::ManagedProfile { account_id }) => orch
            .subscriptions()
            .claude_session_credential(orch, account_id)
            .map(|credential| (credential.env, Some(credential.generation))),
        None => Err(NO_CLAUDE_CREDENTIAL.to_string()),
    }
}

/// The non-secret environment of a Claude launch: the run identity its MCP
/// entry reports, and the git identity its commits are authored with.
fn claude_launch_env(
    run_id: &str,
    user: &crate::identity::UserIdentity,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("CAIRN_RUN_ID".to_string(), run_id.to_string()),
        ("GIT_AUTHOR_NAME".to_string(), user.name.clone()),
        ("GIT_AUTHOR_EMAIL".to_string(), user.email.clone()),
        ("GIT_COMMITTER_NAME".to_string(), user.name.clone()),
        ("GIT_COMMITTER_EMAIL".to_string(), user.email.clone()),
    ])
}

#[cfg(test)]
mod tests {
    use super::{
        arm_claude_watchdog_lease, disarm_claude_watchdog_lease, record_claude_watchdog_lifecycle,
        should_confirm_backend_id_after_init, update_claude_watchdog_lease, NO_CLAUDE_CREDENTIAL,
    };
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    async fn durable_claude_watchdog_records_tool_suppression_and_disarm() {
        use crate::runs::watchdog_ledger::{
            get_watchdog_lease, list_watchdog_lifecycle, WatchdogLeaseState, WatchdogLifecycleKind,
            WatchdogPhase,
        };

        let db = Arc::new(crate::storage::migrated_test_db("claude-watchdog-lifecycle").await);
        db.execute_script(
            "PRAGMA foreign_keys = OFF;
             INSERT INTO sessions (id, chat_id, backend, status, sequence, created_at, updated_at)
               VALUES ('session', 'chat', 'claude', 'open', 1, 1, 1);
             INSERT INTO runs (id, session_id, status, created_at, updated_at)
               VALUES ('run', 'session', 'live', 1, 1);
             PRAGMA foreign_keys = ON;",
        )
        .await
        .unwrap();

        let identity =
            arm_claude_watchdog_lease(db.clone(), "run", "session", "turn", "boot").unwrap();
        update_claude_watchdog_lease(db.clone(), &identity, WatchdogPhase::ToolOutstanding);
        record_claude_watchdog_lifecycle(
            db.clone(),
            &identity,
            WatchdogLifecycleKind::PhaseTransition,
            Some(WatchdogPhase::ToolOutstanding),
            "tool_outstanding",
        );

        let lease = get_watchdog_lease(&db, "run", "session", "turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.phase, WatchdogPhase::ToolOutstanding);
        assert!(lease.phase_deadline_at < i64::MAX);

        disarm_claude_watchdog_lease(db.clone(), &identity, "reader_shutdown");
        let lease = get_watchdog_lease(&db, "run", "session", "turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.state, WatchdogLeaseState::Disarmed);
        assert_eq!(
            list_watchdog_lifecycle(&db, &identity)
                .await
                .unwrap()
                .into_iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
            vec![
                WatchdogLifecycleKind::Armed,
                WatchdogLifecycleKind::PhaseTransition,
                WatchdogLifecycleKind::Disarmed,
            ]
        );
    }

    #[test]
    fn missing_claude_credential_copy_describes_actual_absence_only() {
        assert!(NO_CLAUDE_CREDENTIAL.contains("No Claude account is configured"));
        assert!(!NO_CLAUDE_CREDENTIAL.contains("CLI"));
        assert!(!NO_CLAUDE_CREDENTIAL.contains("available for this session"));
    }
    use crate::backends::SessionStart;

    #[test]
    fn claude_confirms_backend_id_for_new_sessions() {
        assert!(should_confirm_backend_id_after_init(&SessionStart::New {
            session_id: "session-new".to_string(),
        }));
    }

    #[test]
    fn claude_does_not_reconfirm_backend_id_for_resumed_sessions() {
        assert!(!should_confirm_backend_id_after_init(
            &SessionStart::Resume {
                session_id: "session-resume".to_string(),
                backend_id: "backend-existing".to_string(),
            }
        ));
    }

    #[test]
    fn claude_confirms_backend_id_for_forked_sessions() {
        assert!(should_confirm_backend_id_after_init(&SessionStart::Fork {
            session_id: "session-fork".to_string(),
            source_backend_id: "backend-source".to_string(),
        }));
    }
}

/// Parse only the CLI's known limit sentence. Callers must first establish an
/// actual terminal HTTP 429; arbitrary error prose is not retry evidence.
fn parse_claude_reset_boundary(text: &str, now: i64) -> Option<i64> {
    let clause = [
        "You've hit your session limit",
        "You've hit your usage limit",
        "You've hit your weekly limit",
    ]
    .into_iter()
    .find_map(|prefix| text.trim().strip_prefix(prefix))?
    .trim()
    .strip_prefix('·')?
    .trim()
    .strip_prefix("resets ")?;
    let (clock, zone) = match clause.split_once(" (") {
        Some((clock, zone)) => (clock, Some(zone.strip_suffix(')')?)),
        None => (clause, None),
    };
    let (digits, meridiem) = if let Some(clock) = clock.strip_suffix("am") {
        (clock, Some(false))
    } else if let Some(clock) = clock.strip_suffix("pm") {
        (clock, Some(true))
    } else {
        (clock, None)
    };
    let (hour, minute) = match digits.split_once(':') {
        Some((hour, minute)) if minute.len() == 2 => (hour, minute),
        None if meridiem.is_some() => (digits, "00"),
        _ => return None,
    };
    if hour.is_empty()
        || hour.len() > 2
        || !hour
            .bytes()
            .chain(minute.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut hour: u32 = hour.parse().ok()?;
    let minute: u32 = minute.parse().ok()?;
    if let Some(pm) = meridiem {
        if !(1..=12).contains(&hour) {
            return None;
        }
        hour = hour % 12 + if pm { 12 } else { 0 };
    }
    let time = chrono::NaiveTime::from_hms_opt(hour, minute, 0)?;
    match zone {
        Some(zone) => next_reset_in_zone(time, now, zone.parse::<chrono_tz::Tz>().ok()?),
        None => next_reset_in_zone(time, now, chrono::Local),
    }
}

fn next_reset_in_zone<T: chrono::TimeZone>(
    time: chrono::NaiveTime,
    now: i64,
    zone: T,
) -> Option<i64> {
    use chrono::TimeZone;
    let today = chrono::Utc
        .timestamp_opt(now, 0)
        .single()?
        .with_timezone(&zone)
        .date_naive();
    let latest = now.checked_add(7 * 24 * 60 * 60)?;
    for days in 0..=7 {
        let date = today.checked_add_days(chrono::Days::new(days))?;
        let local = zone.from_local_datetime(&date.and_time(time));
        // On a fall-back day both wall-clock occurrences are possible; use the
        // earliest still-future one. A skipped spring-forward time is not guessed.
        let candidates = match local {
            chrono::LocalResult::Single(value) => vec![value.timestamp()],
            chrono::LocalResult::Ambiguous(a, b) => vec![a.timestamp(), b.timestamp()],
            chrono::LocalResult::None => return None,
        };
        if let Some(at) = candidates
            .into_iter()
            .filter(|at| *at > now && *at <= latest)
            .min()
        {
            return Some(at);
        }
    }
    None
}

/// Fail the account over when policy allows, then yield the refused turn and arm
/// the hold that owns it. Returns whether the account's health was persisted.
/// On `Err` neither the yield nor the hold landed, and the caller settles the
/// turn as the refusal it observed.
fn hold_claude_provider_limit(
    host: &ClaudeReaderHost,
    report: ProviderLimitReport<'_>,
) -> Result<bool, String> {
    use crate::orchestrator::wakes::{
        yield_and_arm_provider_limit_hold, ProviderLimitObservation, RESUME_GATE_TOOL_IN_FLIGHT,
    };
    let ProviderLimitReport {
        result_text: text,
        depletion,
        block_recorded,
        blocking_reset,
        tool_in_flight,
        now,
    } = report;
    let parsed = text.and_then(|text| parse_claude_reset_boundary(text, now));
    let (reset, source) = if let Some(reset) = blocking_reset {
        (Some(reset), "rate_limit_event")
    } else if let Some(reset) = parsed {
        (Some(reset), "result_text")
    } else {
        (None, "unknown")
    };
    let credits = matches!(depletion, ProviderDepletion::Credits);
    // Fail over before holding: a limit on one account is a reason to wait only
    // when no other eligible account can take the work. The declared reset is
    // the account's quarantine either way.
    let rotation = rotate_depleted_account(
        &host.orch,
        &host.run_db,
        &host.run_id,
        host.session_id.as_deref(),
        if credits {
            ProviderDepletion::Credits
        } else {
            ProviderDepletion::RollingWindow { resets_at: reset }
        },
        block_recorded,
    );
    let quarantined = rotation.is_some();
    let switched_to = rotation
        .as_ref()
        .and_then(|rotation| rotation.replacement.clone());
    let explicitly_pinned = rotation
        .as_ref()
        .is_some_and(|rotation| rotation.explicitly_pinned);
    let (mut reset, source) = if switched_to.is_some() {
        (Some(now), "account_switch")
    } else if credits {
        // Credits do not refill on a clock Cairn can see.
        (None, "credits_depleted")
    } else {
        (reset, source)
    };
    // No account can take the work now. Under Automatic policy the wait ends
    // when the first account it may select can serve again, which is this
    // account's own reset only when no other comes sooner. An explicit pin
    // waits on its own account.
    let mut boundary_account: Option<(String, String)> = None;
    if let Some(rotation) = rotation
        .as_ref()
        .filter(|rotation| rotation.replacement.is_none() && !rotation.explicitly_pinned)
    {
        if let crate::credentials::RoutedAvailability::At(at, account) =
            host.orch.routed_availability(
                RoutedProvider::Claude,
                rotation.target.project_id.as_deref(),
                rotation.explicit_policy.as_deref(),
                rotation.target.model.as_deref(),
                now,
            )
        {
            if account != rotation.target.account_id {
                reset = Some(at);
                let label = account_label(&rotation.accounts, &account);
                boundary_account = Some((account, label));
            }
        }
    }
    let db = host.run_db.clone();
    let run = host.run_id.clone();
    let (job_id, turn_id, pinned_account) = run_backend_db(CLAUDE_BACKEND_NAME, async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT r.job_id, t.id, s.account_id FROM runs r
                 JOIN turns t ON t.run_id = r.id
                 LEFT JOIN sessions s ON s.id = r.session_id
                 WHERE r.id = ?1 ORDER BY t.sequence DESC LIMIT 1",
                        (run.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::internal("provider hold has no owning turn"))?;
                Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?))
            })
        })
        .await
        .map_err(|error| error.to_string())
    })?;
    // The session pin may already name the replacement; the hold belongs to the
    // account that was refused.
    let account_id = rotation
        .as_ref()
        .map(|rotation| rotation.target.account_id.clone())
        .or_else(|| host.orch.process_state.get_account_id(&host.run_id))
        .or(pinned_account);
    // Hold even when no account is registered: identity uncertainty is not success.
    let observation = ProviderLimitObservation {
        job_id,
        run_id: host.run_id.clone(),
        turn_id: turn_id.clone(),
        backend: "claude".to_string(),
        account_id: account_id.clone(),
        retry_not_before: reset,
        boundary_source: source.to_string(),
        observed_text: text.map(str::to_string),
        resume_gate: tool_in_flight.then_some(RESUME_GATE_TOOL_IN_FLIGHT),
        boundary_account_id: boundary_account.as_ref().map(|(id, _)| id.clone()),
        now,
    };
    let db = host.run_db.clone();
    let emitter = host.orch.services.emitter.clone();
    let held_turn = turn_id.clone();
    let armed = run_backend_db(CLAUDE_BACKEND_NAME, async move {
        let hold = yield_and_arm_provider_limit_hold(&db, observation).await?;
        let change = cairn_db::changes::turn_db_change_for_id(&db, &held_turn, "update").await;
        let _ = emitter.emit("db-change", change);
        Ok::<_, String>(hold)
    });
    let hold = armed?;
    crate::orchestrator::lifecycle::warm_provider_held_run(&host.orch, &host.run_id);
    // The warm process carries the exhausted account's credentials; the resumed
    // turn must cold-start on the replacement pin.
    if switched_to.is_some()
        && matches!(
            host.orch.process_state.get_occupancy(&host.run_id),
            Some(crate::agent_process::process::RunOccupancy::Idle)
        )
    {
        crate::orchestrator::lifecycle::release_and_stop_process(&host.orch, &host.run_id);
    }
    if hold.observed_count == 1 {
        let account = rotation
            .as_ref()
            .map(|rotation| rotation.old_label.clone())
            .unwrap_or_else(|| "Claude".to_string());
        let stamp = |at: i64| crate::clock::stamp(at).unwrap_or_else(|| at.to_string());
        let content = if tool_in_flight {
            // Account bookkeeping and the repin above still happened; only the
            // automatic resume is withheld.
            let why = if credits {
                "has no usage credits left"
            } else {
                "reached its usage limit"
            };
            let resume = match (&switched_to, reset) {
                (Some((_, to)), _) => {
                    format!("Switched this session to {to}; continue once to resume on it.")
                }
                (None, Some(at)) => format!(
                    "Continue once to resume after the provider reset at {}.",
                    stamp(at)
                ),
                (None, None) => {
                    "Continue once to resume when the account is available again.".to_string()
                }
            };
            format!("{account} {why} while a tool call was still waiting for its result, so Cairn did not resume automatically: resuming could run that tool call again. {resume}")
        } else if let Some((_, to)) = &switched_to {
            format!("{account} reached its usage limit. Switched this session to {to}; the unfinished work will continue on it automatically.")
        } else if explicitly_pinned {
            let why = if credits {
                "has no usage credits left"
            } else {
                "reached its usage limit"
            };
            match reset {
                Some(at) => format!("The Claude account chosen for this execution, {account}, {why}. Work is held until its reset at {}, then will continue automatically. To resume sooner, choose another account or Automatic, then continue.", stamp(at)),
                None => format!("The Claude account chosen for this execution, {account}, {why}. Work remains held; choose another account or Automatic, then continue."),
            }
        } else {
            let none_left = if quarantined {
                " and no other eligible Claude account is available"
            } else {
                ""
            };
            let why = if credits {
                "has no usage credits left"
            } else {
                "reached its usage limit"
            };
            match reset {
                Some(at) if boundary_account.is_some() => {
                    let (_, first) = boundary_account.as_ref().expect("checked above");
                    format!("{account} {why}{none_left}. Work is held until {first} resets at {}, then account selection runs again and the work continues automatically.", stamp(at))
                }
                _ if credits => format!("{account} has no usage credits left{none_left}. Work remains held until an operator continues it."),
                Some(at) => format!("{account} reached its usage limit{none_left}. Work is held until the provider reset at {}, then will continue automatically.", stamp(at)),
                None => format!("{account} reached its usage limit{none_left}, without a readable reset time. Work remains held until an operator continues it."),
            }
        };
        let (to_id, to_label) = switched_to.clone().unzip();
        crate::messages::transcript::insert_system_message_sync(
            &host.orch,
            &host.run_id,
            host.session_id.as_deref(),
            Some(&turn_id),
            &content,
            serde_json::json!({"provider":"claude","kind":"provider_limit_hold",
                "retryNotBefore":reset,"boundarySource":source,
                "depletionKind": if credits { "credits" } else { "rolling_window" },
                "fromAccountId":account_id,
                "fromAccountLabel":rotation.as_ref().map(|rotation| rotation.old_label.clone()),
                "toAccountId":to_id,"toAccountLabel":to_label,
                "explicitlyPinned":explicitly_pinned,
                "boundaryAccountId":boundary_account.as_ref().map(|(id, _)| id.clone()),
                "resumeGate":tool_in_flight.then_some(RESUME_GATE_TOOL_IN_FLIGHT)}),
        )?;
    }
    Ok(quarantined)
}

#[cfg(test)]
mod reset_boundary_tests {
    use super::parse_claude_reset_boundary as parse;
    use chrono::{TimeZone, Timelike};

    #[test]
    fn recorded_reset_resolves_in_the_declared_zone() {
        assert_eq!(
            parse(
                "You've hit your session limit · resets 2:40pm (America/New_York)",
                1788545269
            ),
            Some(1788547200)
        );
        assert_eq!(
            parse(
                "You've hit your session limit · resets 2:40pm (America/Los_Angeles)",
                1788545269
            ),
            Some(1788558000)
        );
        assert_eq!(
            parse(
                "You've hit your usage limit · resets 14:40 (America/New_York)",
                1788545269
            ),
            Some(1788547200)
        );
    }

    #[test]
    fn missing_zone_uses_host_wall_clock() {
        let now = chrono::Local
            .with_ymd_and_hms(2026, 9, 4, 10, 0, 0)
            .single()
            .unwrap();
        let result = parse(
            "You've hit your session limit · resets 11am",
            now.timestamp(),
        )
        .unwrap();
        let local = chrono::Local.timestamp_opt(result, 0).single().unwrap();
        assert_eq!(local.date_naive(), now.date_naive());
        assert_eq!(local.hour(), 11);
    }

    #[test]
    fn elapsed_reset_rolls_to_the_next_day_and_noon_midnight_are_distinct() {
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 9, 4, 14, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(
            parse("You've hit your session limit · resets 12am (UTC)", now),
            Some(
                chrono::Utc
                    .with_ymd_and_hms(2026, 9, 5, 0, 0, 0)
                    .unwrap()
                    .timestamp()
            )
        );
        assert_eq!(
            parse("You've hit your session limit · resets 12pm (UTC)", now),
            Some(
                chrono::Utc
                    .with_ymd_and_hms(2026, 9, 5, 12, 0, 0)
                    .unwrap()
                    .timestamp()
            )
        );
    }

    #[test]
    fn arbitrary_prose_invalid_clock_and_unknown_zone_are_uncertainty() {
        for text in [
            "You've hit your usage limit",
            "An arbitrary error resets 2:40pm (America/New_York)",
            "You've hit your session limit · resets 25:00 (UTC)",
            "You've hit your session limit · resets 0pm (UTC)",
            "You've hit your session limit · resets 1:5pm (UTC)",
            "You've hit your session limit · resets 2:60pm (UTC)",
            "You've hit your session limit · resets 2pm (Unknown/Zone)",
            "You've hit your session limit · resets 2pm (UTC) extra",
        ] {
            assert_eq!(parse(text, 1788545269), None, "{text}");
        }
    }

    #[test]
    fn ambiguous_reset_uses_next_occurrence_and_skipped_time_is_unknown() {
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 11, 1, 5, 45, 0)
            .unwrap()
            .timestamp();
        assert_eq!(
            parse(
                "You've hit your session limit · resets 1:30am (America/New_York)",
                now
            ),
            Some(
                chrono::Utc
                    .with_ymd_and_hms(2026, 11, 1, 6, 30, 0)
                    .unwrap()
                    .timestamp()
            )
        );
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 3, 8, 6, 0, 0)
            .unwrap()
            .timestamp();
        assert_eq!(
            parse(
                "You've hit your session limit · resets 2:30am (America/New_York)",
                now
            ),
            None
        );
    }
}

#[cfg(test)]
mod account_reach_tests {
    //! A hold with no account to fail over to waits for the first account its
    //! policy may select, and re-arms rather than launching into a refusal.
    use super::*;
    use crate::credentials::{AccountBlock, ApiProvider, ProviderAuth};
    use crate::orchestrator::wakes::{
        load_active_provider_limit_holds, resolve_provider_limit_hold, ProviderLimitHold,
    };

    struct Fixture {
        orch: Orchestrator,
        db: Arc<LocalDb>,
        refused: String,
        other: String,
        now: i64,
    }

    async fn fixture(name: &str) -> Fixture {
        fixture_with(name, None).await
    }

    /// [`fixture`], with an Anthropic API key configured beside the two
    /// subscription accounts when `api_key` is given.
    async fn fixture_with(name: &str, api_key: Option<&str>) -> Fixture {
        use crate::identity::CredentialStoreExt;
        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',1,'I','active',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,uri_segment,current_session_id,model,created_at,updated_at)
               VALUES('a','p','i','running','builder','builder','session','opus',1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('session','a','claude','native','open',1,1,1);
             INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('run','p','i','a','session','live','resume',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('turn','session','run','a',1,'running','initial',1,1);
             UPDATE jobs SET current_turn_id='turn' WHERE id='a';",
        )
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(crate::db::DbState::new(
            db.clone(),
            Arc::new(crate::storage::SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        let store = crate::credentials::CredentialStore::from_user_identity(
            &crate::identity::UserIdentity {
                user_id: "u".to_string(),
                email: "u@example.com".to_string(),
                name: "U".to_string(),
                claude_auth: api_key
                    .map(|key| crate::credentials::ClaudeAuth::ApiKey(key.to_string())),
                codex_auth: None,
                github_token: None,
            },
        );
        let orch = Orchestrator::builder(
            state,
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root,
        )
        .credential_store(Some(store))
        .build();
        let [refused, other] = ["Work Max", "Personal Max"].map(|label| {
            orch.add_account(
                ApiProvider::Anthropic,
                label.to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap()
            .id
        });
        db.execute(
            "UPDATE sessions SET account_id=?1 WHERE id='session'",
            params![refused.as_str()],
        )
        .await
        .unwrap();
        Fixture {
            orch,
            db,
            refused,
            other,
            now: chrono::Utc::now().timestamp(),
        }
    }

    const HOUR: i64 = 60 * 60;

    /// The refused account resets at T+2h; the other is already limited until
    /// T+1h. A 429 arrives on the refused one, declaring its reset.
    async fn limit_both(f: &Fixture) -> ProviderLimitHold {
        f.orch
            .record_account_health(
                &f.other,
                Vec::new(),
                Some(AccountBlock::Declared(f.now + HOUR)),
            )
            .unwrap();
        let runtime = claude_runtime(
            &f.orch,
            &f.orch.services.emitter,
            "run",
            Some("session".into()),
            f.db.clone(),
        );
        dispatch_claude_result(
            runtime.host.as_ref(),
            true,
            &serde_json::json!({"type":"result","is_error":true,"api_error_status":429,
                                "result":"You've hit your session limit"}),
            ClaudeResultContext {
                blocking_reset: Some(f.now + 2 * HOUR),
                ..Default::default()
            },
            f.now,
        );
        let mut holds = load_active_provider_limit_holds(&f.db).await.unwrap();
        assert_eq!(holds.len(), 1, "the refusal arms one hold");
        holds.remove(0)
    }

    fn blocked_until(f: &Fixture, account: &str) -> Option<i64> {
        f.orch
            .get_credential_store()
            .unwrap()
            .accounts
            .into_iter()
            .find(|candidate| candidate.id == account)
            .and_then(|candidate| candidate.health)
            .and_then(|health| health.blocked_until)
    }

    async fn pinned(f: &Fixture) -> Option<String> {
        crate::sessions::queries::get(&f.db, "session")
            .await
            .unwrap()
            .account_id
    }

    // Acceptance 4. Due at the other account's T+1h, not the refused one's
    // T+2h; at resolution selection runs again and picks the other account,
    // while each account keeps its own reset.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_accounts_limited_waits_for_the_earliest_reset_and_reselects() {
        let f = fixture("earliest-reset").await;
        let hold = limit_both(&f).await;
        assert_eq!(hold.retry_not_before, Some(f.now + HOUR));
        assert_eq!(hold.boundary_account_id.as_deref(), Some(f.other.as_str()));
        assert_eq!(hold.boundary_source, "rate_limit_event");
        assert_eq!(hold.account_id.as_deref(), Some(f.refused.as_str()));
        let due = hold.next_due_at().unwrap();
        assert!((f.now + HOUR..=f.now + HOUR + 120).contains(&due), "{due}");
        assert_eq!(blocked_until(&f, &f.refused), Some(f.now + 2 * HOUR));
        assert_eq!(pinned(&f).await.as_deref(), Some(f.refused.as_str()));

        // The other account's reset passes.
        f.orch
            .record_account_health(&f.other, Vec::new(), None)
            .unwrap();
        // Test builds cannot spawn a managed profile, so the launch itself may
        // be refused after the claim; selection has run by then.
        let _ = resolve_provider_limit_hold(&f.orch, &hold, due).await;
        assert_eq!(
            f.db.query_opt_i64(
                "SELECT COUNT(*) FROM turns WHERE predecessor_id='turn' AND start_reason='wait_resolved'",
                (),
            )
            .await
            .unwrap(),
            Some(1),
            "the due hold claimed its successor"
        );
        assert_eq!(pinned(&f).await.as_deref(), Some(f.other.as_str()));
        assert_eq!(
            blocked_until(&f, &f.refused),
            Some(f.now + 2 * HOUR),
            "the refused account keeps its own reset"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_explicit_pin_waits_on_its_own_reset() {
        let f = fixture("explicit-own-reset").await;
        f.orch
            .set_project_overrides(
                "p",
                Some(crate::credentials::AccountOverrides {
                    anthropic_account_id: Some(f.refused.clone()),
                    ..Default::default()
                }),
            )
            .unwrap();
        let hold = limit_both(&f).await;
        assert_eq!(hold.retry_not_before, Some(f.now + 2 * HOUR));
        assert_eq!(hold.boundary_account_id, None);
    }

    async fn successors(f: &Fixture) -> Option<i64> {
        f.db.query_opt_i64(
            "SELECT COUNT(*) FROM turns WHERE predecessor_id='turn' AND start_reason='wait_resolved'",
            (),
        )
        .await
        .unwrap()
    }

    // The gate asks launch routing, which keeps a healthy pin before it consults
    // policy. A policy changed to an account that cannot serve does not hold
    // back a session whose own account has recovered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_healthy_pin_launches_though_the_policy_names_an_unavailable_account() {
        let f = fixture("healthy-pin-launches").await;
        let hold = limit_both(&f).await;
        f.orch
            .set_project_overrides(
                "p",
                Some(crate::credentials::AccountOverrides {
                    anthropic_account_id: Some(f.other.clone()),
                    ..Default::default()
                }),
            )
            .unwrap();
        f.orch
            .record_account_health(
                &f.other,
                Vec::new(),
                Some(AccountBlock::Declared(f.now + 3 * HOUR)),
            )
            .unwrap();
        f.orch
            .record_account_health(&f.refused, Vec::new(), None)
            .unwrap();

        let _ = resolve_provider_limit_hold(&f.orch, &hold, hold.next_due_at().unwrap()).await;
        assert_eq!(successors(&f).await, Some(1), "the healthy pin launches");
        assert_eq!(pinned(&f).await.as_deref(), Some(f.refused.as_str()));
    }

    // Under Automatic, routing falls back to a configured API key when no
    // subscription can serve, so the hold launches rather than waiting on one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_api_key_fallback_launches_under_automatic() {
        let f = fixture_with("api-key-fallback-launches", Some("test-key")).await;
        let hold = limit_both(&f).await;
        f.orch
            .record_account_health(
                &f.other,
                Vec::new(),
                Some(AccountBlock::Declared(f.now + 3 * HOUR)),
            )
            .unwrap();

        let _ = resolve_provider_limit_hold(&f.orch, &hold, hold.next_due_at().unwrap()).await;
        assert_eq!(
            successors(&f).await,
            Some(1),
            "the API-key fallback launches"
        );
    }

    // The boundary named an account that was limited again before it came due.
    // Launching would only burn an attempt on a refusal, so the hold re-arms to
    // the next account that can serve, here the refused one's own reset.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stale_boundary_rearms_instead_of_burning_launch_attempts() {
        let f = fixture("stale-boundary").await;
        let hold = limit_both(&f).await;
        f.orch
            .record_account_health(
                &f.other,
                Vec::new(),
                Some(AccountBlock::Declared(f.now + 3 * HOUR)),
            )
            .unwrap();
        let due = hold.next_due_at().unwrap();
        assert!(!resolve_provider_limit_hold(&f.orch, &hold, due)
            .await
            .unwrap());

        let rearmed = load_active_provider_limit_holds(&f.db)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(rearmed.id, hold.id);
        assert_eq!(rearmed.retry_not_before, Some(f.now + 2 * HOUR));
        assert_eq!(rearmed.boundary_account_id, None);
        assert_eq!(rearmed.launch_attempts, 0);
        assert_eq!(
            f.db.query_opt_i64("SELECT COUNT(*) FROM turns WHERE job_id='a'", ())
                .await
                .unwrap(),
            Some(1),
            "nothing was claimed"
        );

        // With every account blocked on a guess, there is no time to wait for.
        for account in [&f.refused, &f.other] {
            f.orch
                .record_account_health(
                    account,
                    Vec::new(),
                    Some(AccountBlock::Reprobe(f.now + 4 * HOUR)),
                )
                .unwrap();
        }
        let due = rearmed.next_due_at().unwrap();
        assert!(!resolve_provider_limit_hold(&f.orch, &rearmed, due)
            .await
            .unwrap());
        let timerless = load_active_provider_limit_holds(&f.db)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(timerless.retry_not_before, None);
        assert_eq!(timerless.next_due_at(), None);
        assert_eq!(timerless.launch_attempts, 0);
    }
}

/// Whether a stored `runs.status` names a run that still owns its process.
/// Parsed through [`RunStatus`] so legacy spellings map to their canonical
/// state and no status literal is compared here.
fn stored_run_is_active(run_status: Option<&str>) -> bool {
    run_status
        .and_then(|status| status.parse::<RunStatus>().ok())
        .is_some_and(|status| status.is_active())
}

fn should_finalize_task_run_on_terminal_tool_eof(
    terminal_tool_called: bool,
    run_status: Option<&str>,
    is_task_spawned: bool,
) -> bool {
    terminal_tool_called && stored_run_is_active(run_status) && is_task_spawned
}

/// How a Claude process's stdout-EOF should finalize its run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EofVerdict {
    /// Completed: host-warmed, produced a terminal Result, or a terminal-tool
    /// task that exited before its Result. Finalize Exited (turn marked complete).
    Exited,
    /// Exited mid-run without a terminal Result while still running. Finalize
    /// Crashed, which *interrupts* the running turn rather than failing it —
    /// leaving the job resumable (Pending), not completed-and-advanced and not
    /// cascade-failed. A blocking rate-limit exit lands here too, and is held
    /// rather than crashed when its hold can be armed.
    Crashed,
    /// Already terminal before EOF; nothing to finalize.
    AlreadyTerminal,
}

/// Classify an EOF outcome from process/run facts. Pure so the matrix is
/// unit-testable; the reader thread gathers the inputs and dispatches.
fn classify_eof(
    was_warm: bool,
    saw_terminal_result: bool,
    saw_blocking_rate_limit: bool,
    run_status: Option<&str>,
    terminal_tool_called: bool,
    is_task_spawned: bool,
) -> EofVerdict {
    // Host already warmed the process, or it emitted a terminal Result before
    // closing stdout: it completed. Closing stdout afterward is never a crash.
    if was_warm || (saw_terminal_result && !saw_blocking_rate_limit) {
        return EofVerdict::Exited;
    }
    // Task-spawned run that called its terminal tool but exited before a Result.
    if should_finalize_task_run_on_terminal_tool_eof(
        terminal_tool_called,
        run_status,
        is_task_spawned,
    ) {
        return EofVerdict::Exited;
    }
    // No terminal Result and the run is still active: the process died
    // mid-turn. Finalize Crashed, which *interrupts* the running turn rather than
    // completing it — leaving the job resumable (derives Pending), not
    // completed-and-advanced and not cascade-failed. A blocking rate-limit exit
    // lands here too: it is recoverable on reset, and the rate-limit signal
    // refines the diagnostic/error message in the reader thread, NOT the verdict.
    // (Finalizing Exited would apply TurnState::Complete and advance downstream
    // onto work the blocked account never finished.)
    if stored_run_is_active(run_status) {
        return EofVerdict::Crashed;
    }
    EofVerdict::AlreadyTerminal
}

#[derive(Debug)]
struct RateLimitRetryTarget {
    session_id: String,
    project_id: Option<String>,
    account_id: String,
    model: Option<String>,
}

/// The session and account a run's provider outcome belongs to.
///
/// Bound to the run itself, never to the job head: by the time a depletion is
/// handled the job may already point at a successor session. The
/// account is the one whose credentials were frozen into this run's process at
/// launch: the registered process's, else the run's recorded launch account.
/// The session pin is the last resort, because a failover is exactly what moves
/// it away from the account that was refused.
fn rate_limit_retry_target(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
) -> Result<Option<RateLimitRetryTarget>, String> {
    let db = db.clone();
    let run = run_id.to_string();
    let session = session_id.map(str::to_string);
    let row = run_backend_db(CLAUDE_BACKEND_NAME, async move {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT s.id, COALESCE(r.project_id, i.project_id),
                        COALESCE(r.account_id, s.account_id), j.model
                   FROM runs r
                   JOIN jobs j ON j.id = r.job_id
                   LEFT JOIN issues i ON i.id = r.issue_id
                   JOIN sessions s ON s.id = COALESCE(?2, j.current_session_id)
                  WHERE r.id = ?1
                  LIMIT 1",
                        (run.as_str(), session.as_deref()),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some((
                    row.text(0)?,
                    row.opt_text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                )))
            })
        })
        .await
        .map_err(|error| error.to_string())
    })?;
    let Some((session_id, project_id, launch_account_id, model)) = row else {
        return Ok(None);
    };
    let Some(account_id) = orch
        .process_state
        .get_account_id(run_id)
        .or(launch_account_id)
    else {
        return Ok(None);
    };
    Ok(Some(RateLimitRetryTarget {
        session_id,
        project_id,
        account_id,
        model,
    }))
}

fn account_label(accounts: &[crate::credentials::AccountInfo], account_id: &str) -> String {
    accounts
        .iter()
        .find(|account| account.id == account_id)
        .map(|account| account.label.clone())
        .unwrap_or_else(|| account_id.to_string())
}

/// Apply a verified depletion of the turn's pinned account.
///
/// Health is persisted against the exact pinned account before any
/// replacement is chosen. Under Automatic policy the session is repinned by
/// compare-and-set to the healthiest eligible account; under an explicit
/// policy that names the depleted account the pin stays and the session fails
/// closed. The caller settles the turn afterwards (interrupted, with its
/// process stopped), so the next explicit continuation cold-launches on
/// whatever the pin now names. The interrupted turn is never replayed here.
///
/// `health_recorded` is true when this turn's blocking `rate_limit_event`
/// already recorded the provider's own block, which a reset-less re-probe
/// quarantine must not overwrite.
/// What depletion bookkeeping did to a run's account: the depleted account's
/// health is recorded, and the session is repinned to a replacement when routing
/// policy allows one and an eligible account remains.
struct AccountRotation {
    target: RateLimitRetryTarget,
    old_label: String,
    /// `(account id, label)` the session is now pinned to.
    replacement: Option<(String, String)>,
    explicitly_pinned: bool,
    /// The account the routing policy names, if it names one.
    explicit_policy: Option<String>,
    accounts: Vec<crate::credentials::AccountInfo>,
}

/// Record a depleted account's health, then repin the session away from it.
/// Health is written first so routing already sees the account as unavailable
/// when it picks the replacement. `None` when the run has no account or the
/// bookkeeping could not be persisted.
fn rotate_depleted_account(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    depletion: ProviderDepletion,
    health_recorded: bool,
) -> Option<AccountRotation> {
    let Ok(Some(target)) = rate_limit_retry_target(orch, run_db, run_id, session_id) else {
        return None;
    };
    let persisted = match depletion {
        ProviderDepletion::Credits => orch
            .record_account_credit_depletion(&target.account_id, "credits_required".to_string())
            .map(|_| ()),
        ProviderDepletion::RollingWindow { .. } if health_recorded => Ok(()),
        ProviderDepletion::RollingWindow { resets_at } => orch
            .record_account_health(
                &target.account_id,
                Vec::new(),
                Some(match resets_at {
                    Some(at) => AccountBlock::Declared(at),
                    None => AccountBlock::Reprobe(
                        chrono::Utc::now().timestamp()
                            + TERMINAL_429_REPROBE_AFTER.as_secs() as i64,
                    ),
                }),
            )
            .map(|_| ()),
    };
    if let Err(error) = persisted {
        log::warn!("Failed to persist Claude account depletion: {error}");
        return None;
    }

    let accounts = orch.list_accounts(target.project_id.as_deref());
    let old_label = account_label(&accounts, &target.account_id);
    let snapshot_override = crate::orchestrator::session::snapshot_account_override(orch, run_id)
        .ok()
        .flatten();
    let project_override = crate::orchestrator::session::routed_project_override(
        orch,
        RoutedProvider::Claude,
        target.project_id.as_deref(),
    );
    let explicit_policy = snapshot_override.as_deref().or(project_override.as_deref());
    let explicitly_pinned = explicit_policy == Some(target.account_id.as_str());
    let replacement = (!explicitly_pinned)
        .then(|| {
            orch.select_routed_identity(
                RoutedProvider::Claude,
                target.project_id.as_deref(),
                explicit_policy,
                Some(&target.account_id),
                target.model.as_deref(),
            )
        })
        .flatten();
    let replacement = if let Some((replacement_id, _)) = replacement {
        let repinned = crate::storage::run_db_blocking({
            let db = run_db.clone();
            let session_id = target.session_id.clone();
            let expected_id = target.account_id.clone();
            let replacement_id = replacement_id.clone();
            move || async move {
                crate::sessions::queries::compare_and_set_account_id(
                    &db,
                    &session_id,
                    &expected_id,
                    &replacement_id,
                )
                .await
            }
        });
        match repinned {
            Ok(true) => {
                let label = account_label(&accounts, &replacement_id);
                Some((replacement_id, label))
            }
            // A concurrent repin won; report the durable winner.
            Ok(false) => {
                let winner = crate::storage::run_db_blocking({
                    let db = run_db.clone();
                    let session_id = target.session_id.clone();
                    move || async move { crate::sessions::queries::get(&db, &session_id).await }
                })
                .ok()
                .and_then(|session| session.account_id);
                winner.map(|id| {
                    let label = account_label(&accounts, &id);
                    (id, label)
                })
            }
            Err(error) => {
                log::warn!("Failed to atomically repin depleted Claude account: {error}");
                return None;
            }
        }
    } else {
        None
    };
    let explicit_policy = explicit_policy.map(str::to_string);
    Some(AccountRotation {
        target,
        old_label,
        replacement,
        explicitly_pinned,
        explicit_policy,
        accounts,
    })
}

/// Look up `(job_id, execution_id)` for a run, for the crash diagnostic.
fn run_job_execution(db: &Arc<LocalDb>, run_id: &str) -> (Option<String>, Option<String>) {
    let db = db.clone();
    let run_id = run_id.to_string();
    run_backend_db(CLAUDE_BACKEND_NAME, async move {
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT jobs.id, jobs.execution_id
                         FROM runs INNER JOIN jobs ON jobs.id = runs.job_id
                         WHERE runs.id = ?1",
                        params![run_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok::<_, DbError>((row.opt_text(0)?, row.opt_text(1)?)))
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten()
    .unwrap_or((None, None))
}

/// Claude CLI agent backend.
pub struct ClaudeBackend;

fn should_confirm_backend_id_after_init(session_start: &crate::backends::SessionStart) -> bool {
    matches!(
        session_start,
        crate::backends::SessionStart::New { .. } | crate::backends::SessionStart::Fork { .. }
    )
}

impl AgentBackend for ClaudeBackend {
    fn name(&self) -> &str {
        "Claude"
    }

    fn is_available(&self) -> Result<(), String> {
        crate::env::find_binary("claude").map(|_| ())
    }

    fn serves(
        &self,
        _orch: &Orchestrator,
        _project_id: Option<&str>,
    ) -> Vec<cairn_models::types::catalog::ServedVerb> {
        use cairn_models::types::catalog::{ServedVerb, Wire};
        ServedVerb::conversation(Wire::ClaudeStreamJson)
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        // Whether Claude's models are usable is a property of the configured
        // accounts, so the orchestrator routes Claude through
        // `discover_catalog_blocking` instead.
        Err("Claude discovery requires the configured accounts".to_string())
    }

    fn option_descriptors(&self) -> Vec<ProviderOptionDescriptor> {
        vec![ProviderOptionDescriptor {
            key: ProviderOptionKey::ReasoningEffort,
            label: "Effort".to_string(),
            kind: OptionKind::Enum,
            choices: ["low", "medium", "high", "xhigh", "max"]
                .into_iter()
                .map(|value| OptionChoice {
                    value: value.to_string(),
                    label: value.to_string(),
                })
                .collect(),
            default: None,
        }]
    }

    fn resolve_tools(&self, agent_tools: &[String], agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;

        // Resolve agent tool names to the Cairn allow-list (friendly verbs
        // aliased to Cairn verbs; every other bare name and dead names
        // dropped).
        let mut allowed = toolkits::resolve_tools(agent_tools);

        // Temporary permissions floor: the three core verbs are always allowed
        // for every agent (CAIRN-1172). Without this a read-only agent's
        // `write cairn:~/return` trips the permission prompt and wedges the run.
        toolkits::ensure_core_verbs(&mut allowed);

        // The retired `mcp__cairn__return` tool is no longer injected: returning
        // is `write cairn:~/return`, and schema-constrained calls capture the
        // native structured output server-side (CAIRN-2505). No MCP handler ever
        // dispatched a `return` tool, so it was dead weight on the allow-list.

        // Built-ins are off for every launch (`--tools ""`), so the disallow
        // list is only the MCP tools this agent is denied.
        let mut disallowed = agent_disallowed.to_vec();
        disallowed.sort();
        disallowed.dedup();

        ResolvedTools {
            allowed,
            disallowed,
        }
    }

    fn start_session(
        &self,
        config: SessionConfig,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<(), String> {
        let start_time = std::time::Instant::now();

        let session_id = Some(config.session_start.session_id().to_string());
        let mut event = crate::resume_timing::ResumeTimingEvent::new("claude_prepare_start");
        event.run_id = Some(&config.run_id);
        event.session_id = session_id.as_deref();
        event.mode = Some(if config.session_start.replayed_backend_id().is_some() {
            "resume"
        } else {
            "new"
        });
        event.bytes = Some(config.prompt.len());
        event.emit();

        // Effective model recorded on the process handle for warm-reuse
        // reconciliation (captured before `config.model` is moved into args).
        let effective_model = config.model.as_ref().map(|m| m.as_str().to_string());

        let workspace_instructions = crate::workspace::instructions::read_workspace_instructions();
        let project_instructions = crate::workspace::instructions::read_project_instructions(
            std::path::Path::new(&config.working_dir),
        );

        // Assemble the uniform system-prompt stack (cairn + workspace + project +
        // role + orientation) shared by every backend, then deliver it as ONE
        // file via --system-prompt-file, fully replacing CC's default. The file
        // bytes equal the persisted segment concatenation exactly. Project/local
        // CLAUDE.md native memory (a separate context-injection mechanism) is
        // suppressed via --setting-sources user in the CLI args, so the project
        // instruction layer reaches Claude exactly once — through the assembled
        // `project` segment, not also through CC auto-discovery.
        let prompt_segments = assemble_prompt_segments(
            &crate::system_prompt::cairn_system_prompt(config.ambient),
            workspace_instructions.as_deref(),
            project_instructions.as_deref(),
            config.system_prompt_content.as_deref(),
            config.system_prompt_dynamic_tail.as_deref(),
        );
        // `working_dir` is this run's process residence — the job scratch dir the
        // CLI runs in, which is writable under the fence and reclaimed with the job.
        let system_prompt = system_prompt_placement(
            std::path::Path::new(&config.working_dir),
            &config.run_id,
            flatten_prompt_segments(&prompt_segments),
        );

        persist_system_prompt_event(
            orch,
            &config.run_id,
            session_id.as_deref(),
            "claude",
            &prompt_segments,
        );

        // Resolve the run's owning DB ONCE (CAIRN-2208) and thread it through the
        // transcript/stream/run-state writes below. A team run lives wholly in its
        // synced replica; targeting the private DB would fail the
        // message_streams→runs foreign key. Resolved before the process spawns so a
        // closed replica fails fast rather than orphaning a live agent.
        let run_db = resolve_run_db(CLAUDE_BACKEND_NAME, orch, &config.run_id)?;

        let (mcp_credential_var, mcp_credential) = crate::mcp::run_bearer::mcp_credential_env(
            orch,
            &config.mcp,
            config.mcp_bearer.as_ref(),
        )?;
        log::info!("Using {} for run {}", mcp_credential_var, config.run_id);

        // Inject user identity into Claude process environment
        // Prefer pre-resolved identity from SessionConfig (includes project overrides)
        let identity = config
            .identity
            .as_ref()
            .cloned()
            .or_else(|| orch.get_identity());

        // The Cairn-managed Claude profile, when the identity carries one. It is
        // also where the CLI keeps the conversation a resume replays, so the
        // transcript repair below needs the same path.
        let managed_account = identity.as_ref().and_then(|user| match &user.claude_auth {
            Some(crate::credentials::ClaudeAuth::ManagedProfile { account_id }) => {
                Some(account_id.clone())
            }
            _ => None,
        });
        let claude_config_dir = managed_account
            .as_deref()
            .and_then(|account_id| orch.subscriptions().claude_home(orch, account_id));

        // Fail closed. A session that spawns without an explicit credential
        // runs on whatever account is signed in at the user level: invisible to
        // usage routing, unattributable, and impossible to switch away from
        // when it runs out. Refusing to start says so; starting anyway looks
        // like it worked and quietly spends someone else's subscription.
        let user = identity.ok_or_else(|| NO_CLAUDE_CREDENTIAL.to_string())?;
        let mut secrets = SecretEnv::new().with(mcp_credential_var, mcp_credential);
        let (credential, credential_generation) = claude_launch_credential(orch, &user)?;
        secrets.extend(credential);

        log::info!(
            "Injected user identity into session: {} <{}>",
            user.name,
            user.email
        );

        let bidirectional = config.bidirectional;
        let spec = SessionSpec {
            start: config.session_start.clone(),
            model: config.model.as_ref().map(|model| model.to_string()),
            // Claude's CLI replaced --max-thinking-tokens with --effort. Prefer an
            // explicit effort; fall back to "high" for legacy presets that still
            // only carry a (now-ignored) max_thinking_tokens budget.
            effort: config
                .reasoning_effort
                .clone()
                .or_else(|| config.max_thinking_tokens.map(|_| "high".to_string())),
            // The three verbs are always allow-listed and native tools are
            // disallowed, so no CLI permission prompt is ever needed: escape
            // gating happens inside the verb handlers (the worktree fence).
            approval: super::approval_mode(config.permissions.fence),
            output_schema: config.output_schema.clone(),
            cwd: config.working_dir.clone(),
            env: claude_launch_env(&config.run_id, &user),
            mcp: Some(super::cairn_servers(&config.mcp)),
            secrets,
            account_id: managed_account,
            provider: ProviderSpec::Claude(ClaudeSpec {
                system_prompt,
                // Hook settings for memory surfacing (passed via --settings).
                hook_settings: Some(crate::memories::hooks::hook_settings_file(
                    orch.mcp_callback_port,
                )),
                allowed_tools: config.allowed_tools,
                disallowed_tools: config.disallowed_tools,
                mode: if bidirectional {
                    ClaudeMode::Interactive
                } else {
                    ClaudeMode::OneShot {
                        prompt: config.prompt.clone(),
                    }
                },
            }),
        };

        let mut event =
            crate::resume_timing::ResumeTimingEvent::new("claude_args_ready").elapsed(start_time);
        event.run_id = Some(&config.run_id);
        event.session_id = session_id.as_deref();
        event.emit();

        // A resume hands the CLI its own persisted conversation to replay, and a
        // single content block the API rejects there kills the session for good:
        // every later resume, including a well-formed operator message, fails
        // with the same 400 (CAIRN-3263). Repair it now, while no CLI process
        // holds the file open.
        if let Some(backend_id) = config.session_start.replayed_backend_id() {
            let repair_started = std::time::Instant::now();
            let repair = super::claude_transcript::repair_before_resume(
                claude_config_dir.as_deref(),
                std::path::Path::new(&config.working_dir),
                backend_id,
            );
            let mut event =
                crate::resume_timing::ResumeTimingEvent::new("claude_transcript_repair")
                    .elapsed(repair_started);
            event.run_id = Some(&config.run_id);
            event.session_id = session_id.as_deref();
            event.mode = Some(if repair.used_full_scan {
                "full"
            } else {
                "incremental"
            });
            event.count = Some(repair.lines_parsed);
            event.bytes = Some(repair.bytes_read.min(usize::MAX as u64) as usize);
            event.emit();
        }

        // Check if we need to evict a warm process to make room
        orch.collect_warm_if_needed();
        let spawn_started = std::time::Instant::now();

        let runtime = orch.models_runtime();
        let launched = runtime
            .claude
            .open(runtime.host.as_ref(), spec)
            .map_err(|error| {
                if let SessionError::Spawn(reason) = &error {
                    log::debug!("ClaudeBackend: spawn failed: {}", reason);
                    insert_error_event(
                        orch,
                        &config.run_id,
                        session_id.as_deref(),
                        &format!("Failed to start Claude: {}", reason),
                    );
                }
                error.to_string()
            })?;
        let mut event = crate::resume_timing::ResumeTimingEvent::new("claude_process_spawned")
            .elapsed(spawn_started);
        event.run_id = Some(&config.run_id);
        event.session_id = session_id.as_deref();
        event.emit();

        // Transition run to Running AFTER successful spawn (sets started_at accurately)
        if let Err(e) = transition_run_to_live(CLAUDE_BACKEND_NAME, orch, &run_db, &config.run_id) {
            log::warn!("Failed to transition run to running: {}", e);
            // Job is already Running from start_job's transition_job call — no write needed
        }

        let SessionEvents::ClaudeStream { stdout, stderr } = launched.events else {
            return Err("the Claude CLI opened a session with no stream-json output".to_string());
        };
        let confirm_backend_id_after_init =
            should_confirm_backend_id_after_init(&config.session_start);

        // Drain stderr on its own thread, keeping a joinable handle so the
        // reader thread can read any typed failure it classified before
        // deciding what stdout EOF meant.
        let stderr_watch = stderr.map(StderrWatch::spawn);

        let control = Arc::new(Mutex::new(Some(launched.control)));

        // Get job_id for warm process tracking
        let process_job_id: Option<String> =
            run_job_id(CLAUDE_BACKEND_NAME, &run_db, &config.run_id);

        {
            let mut active_process = crate::agent_process::process::ActiveProcess::new(
                launched.process,
                control.clone(),
                session_id.clone(),
                process_job_id,
            );
            active_process.model = effective_model.clone();
            active_process.account_id = config.account_id.clone();
            record_run_account(
                CLAUDE_BACKEND_NAME,
                &run_db,
                &config.run_id,
                config.account_id.as_deref(),
            );
            let fed_account = launched.facts.account_id.clone();
            active_process.facts = Some(launched.facts);
            // Only a process with a live stdin can take a renewed credential;
            // a one-shot runs out its turn on the one it launched with. If
            // registering the process fails, the handle and its registration
            // drop together.
            active_process.credential_feed = credential_generation
                .filter(|_| bidirectional)
                .zip(fed_account)
                .map(|(generation, account)| {
                    orch.claude_feeds().register(
                        config.run_id.clone(),
                        account,
                        generation,
                        control.clone(),
                    )
                });
            orch.process_state
                .register_process(config.run_id.clone(), active_process)?;
        }

        // In bidirectional mode, the first turn goes in over stdin.
        if bidirectional {
            let mut guard = control.lock().map_err(|e| e.to_string())?;
            if let Some(control) = guard.as_mut() {
                control.open_turn(&config.message_content)?;
                let mut event =
                    crate::resume_timing::ResumeTimingEvent::new("claude_initial_stdin_written")
                        .elapsed(start_time);
                event.run_id = Some(&config.run_id);
                event.session_id = session_id.as_deref();
                event.emit();
                log::info!(
                    "Sent initial prompt via stdin ({} chars)",
                    config.prompt.len()
                );
            }
        }

        // Clone what we need for the reader thread
        let run_id = config.run_id.clone();
        let orch = orch.clone();
        let emitter = orch.services.emitter.clone();

        let thread_session_id = session_id.clone();

        // The reader below reports this process's first output, so a boot-ramp
        // spawn keeps warming past registration until it does. Marked here,
        // before the reader exists and before this start returns, so the
        // promoter never reads it unset.
        crate::execution::jobs::spawn_reports_first_output(&orch, &run_id);

        // Spawn thread to read stdout and emit events
        thread::spawn(move || {
            Self::reader_thread(
                &orch,
                &emitter,
                &run_id,
                thread_session_id,
                confirm_backend_id_after_init,
                stdout,
                stderr_watch,
                run_db,
            );
        });

        let mut event = crate::resume_timing::ResumeTimingEvent::new("claude_reader_started")
            .elapsed(start_time);
        event.run_id = Some(&config.run_id);
        event.session_id = session_id.as_deref();
        event.emit();
        Ok(())
    }

    fn response_completion_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
    ) -> Result<(), String> {
        use cairn_models::interface::session::Cli;
        let selection = select_routed_serving_identity(
            orch,
            RoutedProvider::Claude,
            project_id,
            None,
            SelectionMode::Probe,
        )?;
        orch.models_runtime()
            .host
            .resolve_cli(Cli::Claude)
            .map_err(String::from)?;
        // Asked once per availability question rather than per model: it runs
        // the CLI against the account's home.
        if let Some(crate::credentials::ClaudeAuth::ManagedProfile { account_id }) =
            &selection.identity.claude_auth
        {
            if !orch.subscriptions().claude_home_signed_in(orch, account_id) {
                return Err("The Claude account Cairn would use is not signed in. Re-authenticate it in Settings → Providers.".to_string());
            }
        }
        Ok(())
    }

    fn response_model_availability(
        &self,
        orch: &Orchestrator,
        project_id: Option<&str>,
        model: &str,
    ) -> Result<(), String> {
        select_routed_serving_identity(
            orch,
            RoutedProvider::Claude,
            project_id,
            Some(model),
            SelectionMode::Probe,
        )
        .map(|_| ())
    }

    /// One tool-free round trip: the CLI in print mode on the account routing
    /// chose, with no MCP server, no native tools, and nothing persisted.
    fn complete(
        &self,
        request: CompletionRequest,
        orch: &Orchestrator,
        _admitted: crate::backends::Admitted,
    ) -> Result<CompletionOutcome, CompletionFailure> {
        use super::one_shot::{
            instructions, map_one_shot_error, map_open_error, one_shot_timeout,
            response_scratch_dir,
        };
        use cairn_models::interface::session::{ApprovalMode, PlacedFile, SessionStart};
        use cairn_models::session::one_shot::await_one_shot;

        if request.messages.is_empty() {
            return Err(CompletionError::InvalidRequest(
                "at least one message is required".to_string(),
            )
            .into());
        }
        let selection = select_routed_serving_identity(
            orch,
            RoutedProvider::Claude,
            request.project_id.as_deref(),
            Some(&request.model),
            SelectionMode::Serve,
        )
        .map_err(CompletionError::Upstream)?;
        // From here on the call runs on the routed account, so a failure names
        // it: the CLI's account-level refusals arrive as ordinary errors.
        let account_id = selection.account_id.clone();
        let serve = || -> Result<CompletionOutcome, CompletionError> {
            let (secrets, _) = claude_launch_credential(orch, &selection.identity)
                .map_err(CompletionError::Upstream)?;
            // A subscription's cost figure is notional; only an API key's is spent.
            let metered = matches!(
                selection.identity.claude_auth,
                Some(crate::credentials::ClaudeAuth::ApiKey(_))
            );
            let scratch = response_scratch_dir()?;
            let extras = request.options.clone();
            let spec = SessionSpec {
                start: SessionStart::New {
                    session_id: uuid::Uuid::new_v4().to_string(),
                },
                model: Some(request.model.clone()),
                effort: extras
                    .reasoning_effort
                    .or_else(|| extras.max_thinking_tokens.map(|_| "high".to_string())),
                approval: ApprovalMode::Deny,
                output_schema: request.output_schema.clone(),
                cwd: scratch.path(),
                env: BTreeMap::new(),
                mcp: None,
                secrets,
                account_id: selection.account_id.clone(),
                provider: ProviderSpec::Claude(ClaudeSpec {
                    system_prompt: PlacedFile {
                        dir: scratch.path(),
                        name: "system-prompt.md".to_string(),
                        contents: instructions(&request),
                    },
                    hook_settings: None,
                    allowed_tools: Vec::new(),
                    disallowed_tools: Vec::new(),
                    mode: ClaudeMode::OneShot {
                        prompt: request.single_prompt(),
                    },
                }),
            };
            let started = std::time::Instant::now();
            let runtime = orch.models_runtime();
            let launched = runtime
                .claude
                .open(runtime.host.as_ref(), spec)
                .map_err(map_open_error)?;
            let reply =
                await_one_shot(launched, one_shot_timeout(&request)).map_err(map_one_shot_error)?;
            let latency_ms = started.elapsed().as_millis() as u64;
            drop(scratch);

            // With a schema the CLI's text is a summary; the answer is the value
            // its StructuredOutput tool returned.
            let (text, parsed) = if request.output_schema.is_some() {
                let structured = reply.structured.ok_or_else(|| {
                    CompletionError::InvalidResponse(
                        "Claude returned no structured output for the requested schema".to_string(),
                    )
                })?;
                let text = serde_json::to_string(&structured)
                    .map_err(|error| CompletionError::InvalidResponse(error.to_string()))?;
                (text, Some(structured))
            } else {
                (reply.text, None)
            };
            Ok(CompletionOutcome {
                text,
                parsed,
                model: request.model,
                usage: reply
                    .usage
                    .as_ref()
                    .map(cairn_models::usage::metered)
                    .unwrap_or_default()
                    .with_cost(reply.cost_usd.filter(|_| metered)),
                latency_ms,
                account_id: selection.account_id,
            })
        };
        serve().map_err(CompletionFailure::on_account(account_id))
    }

    fn supports_resume(&self) -> bool {
        true
    }

    /// A fresh start passes `--session-id <cairn session id>`.
    fn fresh_start_claims_session_id(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        true
    }

    fn runtime_launch_capability(
        &self,
        _launch: &crate::backends::RuntimeLaunch,
    ) -> Result<crate::backends::RuntimeLaunchCapability, String> {
        use crate::backends::{
            RuntimeClass, RuntimeHostKind, RuntimeHostRequirement, RuntimeLane,
            RuntimeLaunchCapability, RuntimeResourceClaim,
        };
        Ok(RuntimeLaunchCapability {
            class: RuntimeClass::DedicatedProcess,
            lane: RuntimeLane::new("claude-cli"),
            host_requirement: RuntimeHostRequirement::ResidentOn(RuntimeHostKind::Runner),
            claim: RuntimeResourceClaim {
                resident_process_units: 1,
                server_instance_key: None,
                logical_stream_units: 1,
                estimated_memory_bytes: Some(450 * 1024 * 1024),
            },
            supports_warm_reuse: true,
        })
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        // Claude is permanently CLI-bound: each ephemeral call is a dedicated
        // `claude --print` process (~450 MB RSS). The admission ceiling is the
        // only lever on fan-out memory, so cap concurrency here at a bound
        // derived from physical RAM (CAIRN-2557).
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::DedicatedProcess,
            max_concurrency: Some(*CLAUDE_CALL_MAX_CONCURRENCY),
        }
    }
}

/// Claude's catalog: the shipped Claude models, offered only when a credential
/// Cairn would actually launch a session with can use them.
///
/// That is a configured account — an API key, or a managed profile that is
/// signed in — and never the CLI's own user-level login, which sessions refuse.
/// Asking the ambient CLI instead left a runner whose only credential is a
/// managed profile (every headless one) with no Claude models at all, and
/// wrote a first-run `~/.claude` into a home Cairn otherwise never touches.
pub(crate) fn discover_catalog_blocking(
    orch: &Orchestrator,
) -> (Vec<DiscoveredModel>, Option<String>) {
    match usable_claude_credential(orch) {
        Ok(()) => (cairn_models::types::catalog::claude_models(), None),
        Err(error) => (Vec::new(), Some(error)),
    }
}

fn usable_claude_credential(orch: &Orchestrator) -> Result<(), String> {
    const NO_ACCOUNT: &str =
        "No Claude account is configured. Sign in with a Claude subscription or add an API key.";
    let store = orch.get_credential_store().ok_or(NO_ACCOUNT)?;
    let accounts: Vec<_> = store
        .accounts
        .iter()
        .filter(|account| account.api_provider == crate::credentials::ApiProvider::Anthropic)
        .collect();
    if accounts.iter().any(|account| {
        matches!(
            account.auth,
            crate::credentials::ProviderAuth::ApiKey { .. }
        )
    }) {
        return Ok(());
    }
    let profiles: Vec<_> = accounts
        .iter()
        .filter(|account| {
            matches!(
                account.auth,
                crate::credentials::ProviderAuth::ClaudeProfile { .. }
            )
        })
        .collect();
    if profiles.is_empty() {
        return Err(NO_ACCOUNT.to_string());
    }
    get_claude_path(&orch.process_state)?;
    let signed_in = profiles.iter().any(|account| {
        orch.subscriptions()
            .claude_home_signed_in(orch, &account.id)
    });
    if signed_in {
        Ok(())
    } else {
        Err(
            "No configured Claude profile is signed in. Re-authenticate one to use Claude."
                .to_string(),
        )
    }
}

#[cfg(test)]
mod launch_contract_tests {
    use super::{claude_launch_env, sanitize_mcp_diagnostic, validate_claude_init};
    use crate::backends::SessionStart;
    use crate::credentials::ClaudeAuth;
    use crate::identity::UserIdentity;
    use cairn_models::interface::session::{
        ApprovalMode, ClaudeMode, ClaudeSpec, PlacedFile, ProviderSpec, SecretEnv, SessionSpec,
    };
    use cairn_models::session::claude::ClaudeCli;
    use cairn_models::session::mcp::{McpServer, McpServers};
    use std::collections::{BTreeMap, HashMap};

    fn identity(auth: ClaudeAuth) -> UserIdentity {
        UserIdentity {
            user_id: "user-1".to_string(),
            email: "agent@example.com".to_string(),
            name: "Cairn Agent".to_string(),
            claude_auth: Some(auth),
            codex_auth: None,
            github_token: None,
        }
    }

    fn mcp() -> McpServers {
        McpServers::new(vec![McpServer::cairn_stdio("cairn-cmd", BTreeMap::new())]).unwrap()
    }

    fn spec(start: SessionStart, mode: ClaudeMode, credential: SecretEnv) -> SessionSpec {
        let user = identity(ClaudeAuth::ApiKey("stored-key".to_string()));
        let mut secrets = SecretEnv::new().with("CAIRN_MCP_SECRET", "mcp-secret");
        secrets.extend(credential);
        SessionSpec {
            start,
            model: Some("sonnet".to_string()),
            effort: Some("high".to_string()),
            approval: ApprovalMode::Allow,
            output_schema: None,
            cwd: "/tmp/scratch".to_string(),
            env: claude_launch_env("run-1", &user),
            mcp: Some(mcp()),
            secrets,
            account_id: None,
            provider: ProviderSpec::Claude(ClaudeSpec {
                system_prompt: PlacedFile {
                    dir: "/tmp/scratch/prompts".to_string(),
                    name: "run-1.md".to_string(),
                    contents: "PROMPT".to_string(),
                },
                hook_settings: None,
                allowed_tools: vec![
                    "mcp__cairn__read".to_string(),
                    "mcp__cairn__write".to_string(),
                ],
                disallowed_tools: vec!["mcp__cairn__update_issue".to_string()],
                mode,
            }),
        }
    }

    fn spawned(spec: &SessionSpec) -> (Vec<String>, HashMap<String, String>) {
        let config = ClaudeCli::spawn_config(
            "/usr/local/bin/claude",
            spec,
            Some("/tmp/scratch/prompts/run-1.md".to_string()),
            Some("/home/.cairn/hook-settings.json".to_string()),
        )
        .unwrap();
        assert_eq!(config.program, "/usr/local/bin/claude");
        assert_eq!(config.cwd.as_deref(), Some("/tmp/scratch"));
        (config.args, config.env)
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    /// The environment every Claude launch has spawned with, byte for byte.
    fn expected_env(credential: (&str, &str)) -> HashMap<String, String> {
        [
            ("CAIRN_RUN_ID", "run-1"),
            ("CAIRN_MCP_SECRET", "mcp-secret"),
            ("ENABLE_TOOL_SEARCH", "false"),
            ("CLAUDE_CODE_ENABLE_TASKS", "false"),
            ("MCP_TOOL_TIMEOUT", "604800000"),
            ("GIT_AUTHOR_NAME", "Cairn Agent"),
            ("GIT_AUTHOR_EMAIL", "agent@example.com"),
            ("GIT_COMMITTER_NAME", "Cairn Agent"),
            ("GIT_COMMITTER_EMAIL", "agent@example.com"),
            credential,
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
    }

    #[test]
    fn profile_launch_keeps_inline_mcp_and_uses_only_managed_auth() {
        let (args, env) = spawned(&spec(
            SessionStart::New {
                session_id: "session-1".to_string(),
            },
            ClaudeMode::Interactive,
            SecretEnv::new().with("CLAUDE_CONFIG_DIR", "/managed/claude/profile"),
        ));
        assert_eq!(
            env,
            expected_env(("CLAUDE_CONFIG_DIR", "/managed/claude/profile"))
        );
        let mcp_config = mcp().claude_mcp_config();
        assert_eq!(
            args,
            strings(&[
                "--output-format",
                "stream-json",
                "--mcp-config",
                &mcp_config,
                "--strict-mcp-config",
                "--setting-sources",
                "user",
                "--input-format",
                "stream-json",
                "--effort",
                "high",
                "--model",
                "sonnet",
                "--system-prompt-file",
                "/tmp/scratch/prompts/run-1.md",
                "--settings",
                "/home/.cairn/hook-settings.json",
                "--dangerously-skip-permissions",
                "--allowedTools",
                "mcp__cairn__read,mcp__cairn__write",
                "--disallowedTools",
                "mcp__cairn__update_issue",
                "--session-id",
                "session-1",
                "--print",
                "--verbose",
                "--tools",
                "",
                "--include-partial-messages",
            ])
        );
    }

    #[test]
    fn api_key_launch_retains_the_same_strict_inline_mcp_contract() {
        let (args, env) = spawned(&spec(
            SessionStart::Resume {
                session_id: "session-1".to_string(),
                backend_id: "backend-1".to_string(),
            },
            ClaudeMode::Interactive,
            SecretEnv::new().with("ANTHROPIC_API_KEY", "brokered-key"),
        ));
        assert_eq!(env, expected_env(("ANTHROPIC_API_KEY", "brokered-key")));
        let tail = &args[args.len() - 7..];
        assert_eq!(
            tail,
            strings(&[
                "--resume",
                "backend-1",
                "--print",
                "--verbose",
                "--tools",
                "",
                "--include-partial-messages"
            ])
        );
        assert!(!args.iter().any(|arg| arg == "--session-id"));
    }

    /// Print mode reads its one prompt from stdin, so none is in the arguments.
    #[test]
    fn a_one_shot_launch_carries_no_prompt_argument() {
        let (args, _) = spawned(&spec(
            SessionStart::New {
                session_id: "session-1".to_string(),
            },
            ClaudeMode::OneShot {
                prompt: "-starts-with-dash".to_string(),
            },
            SecretEnv::new().with("ANTHROPIC_API_KEY", "brokered-key"),
        ));
        assert!(!args.iter().any(|arg| arg == "--input-format"));
        assert!(!args
            .iter()
            .any(|arg| arg == "-starts-with-dash" || arg == "--"));
        assert_eq!(
            &args[args.len() - 6..],
            strings(&[
                "session-1",
                "--print",
                "--verbose",
                "--tools",
                "",
                "--include-partial-messages",
            ])
        );
    }

    #[test]
    fn init_with_cairn_tools_succeeds() {
        let init = serde_json::json!({
            "tools": ["mcp__cairn__read", "mcp__cairn__write", "mcp__cairn__run"],
            "mcp_servers": [{"name": "cairn", "status": "connected"}]
        });
        assert!(validate_claude_init(&init, &[]).is_ok());
    }

    #[test]
    fn init_with_connected_server_but_incomplete_inventory_fails_clearly() {
        for tools in [
            serde_json::json!([]),
            serde_json::json!(["Read", "mcp__unrelated__search"]),
            serde_json::json!(["mcp__cairn__read"]),
            serde_json::json!(["mcp__cairn__read", "mcp__cairn__write"]),
        ] {
            let error = validate_claude_init(
                &serde_json::json!({
                    "tools": tools,
                    "mcp_servers": [{"name": "cairn", "status": "connected"}]
                }),
                &[],
            )
            .unwrap_err();
            assert!(error.contains("without the required Cairn MCP tools"));
            assert!(error.contains("connected"));
        }
    }

    #[test]
    fn sanitized_mcp_stderr_is_surfaced_in_init_failure() {
        // This value is deliberately NOT registered. Profile-scoped Claude can
        // print credentials Cairn never held, so exact-value scrubbing is not
        // sufficient at this untrusted stderr boundary.
        let secret = "unregistered-profile-token-4Qf8Jv2Kp9";
        let diagnostic = sanitize_mcp_diagnostic(&format!(
            "MCP server cairn failed with authorization: Bearer {secret}"
        ));
        assert!(!diagnostic.contains(secret));
        let error = validate_claude_init(
            &serde_json::json!({"tools": [], "mcp_servers": []}),
            &[diagnostic],
        )
        .unwrap_err();
        assert!(error.contains("MCP server cairn failed"));
        assert!(!error.contains(secret));
    }
}

#[cfg(test)]
mod stderr_failure_tests {
    use super::{classify_stderr_failure, BackendFailure, StderrWatch};

    fn watch_over(lines: &str) -> Option<BackendFailure> {
        StderrWatch::spawn(Box::new(std::io::Cursor::new(lines.to_string()))).settle()
    }

    #[test]
    fn settle_reports_a_diagnosis_the_process_printed_before_exiting() {
        // The join in `settle` is what makes this deterministic: without it the
        // reader thread could classify EOF before the drain recorded the line.
        assert_eq!(
            watch_over(
                "(node:1) ExperimentalWarning: stream/web\n\
                 No conversation found with session ID: 5c7fe52a-b0bd-4f3b-8bde-d7af9a2b2b93"
            ),
            Some(BackendFailure::SessionUnresolvable)
        );
    }

    #[test]
    fn settle_reports_nothing_for_a_silent_or_ordinary_exit() {
        assert_eq!(watch_over(""), None);
        assert_eq!(watch_over("some unrelated warning\nand another"), None);
    }

    /// CAIRN-3104: the stderr drain must be settled BEFORE the EOF verdict is
    /// classified, so every arm sees a diagnosis the process printed on its way
    /// out. Guarded structurally because the ordering is load-bearing and
    /// hoisting the settle below the match would silently reintroduce the race
    /// between the drain and this thread's exit.
    #[test]
    fn the_stderr_drain_is_settled_before_the_eof_verdict() {
        const READER: &str = include_str!("../../../cairn-session/src/backends/claude.rs");
        let report = READER
            .split("host.host.eof(ClaudeEof {")
            .nth(1)
            .expect("reader reports EOF")
            .split("});")
            .next()
            .unwrap();
        assert!(
            report.contains("backend_failure: stderr_watch.and_then(StderrWatch::settle)"),
            "stderr joins while constructing the report, before the host can classify it"
        );
        const HOST: &str = include_str!("claude.rs");
        let host = HOST
            .split("fn eof(&self, report: ClaudeEof)")
            .nth(1)
            .unwrap();
        assert!(host.contains("match classify_eof("));
    }

    #[test]
    fn extracted_reader_routes_teardown_through_admission_host() {
        const READER: &str = include_str!("../../../cairn-session/src/backends/claude.rs");
        assert_eq!(READER.matches("host.host.stop_process();").count(), 2);
        assert!(!READER.contains("host.process_state.stop_and_remove(run_id)"));
        assert!(!READER.contains("processes.remove(run_id)"));
        const HOST: &str = include_str!("claude.rs");
        let stop = HOST
            .rsplit("fn stop_process(&self) {")
            .next()
            .unwrap()
            .split("fn startup_rejected")
            .next()
            .unwrap();
        assert!(stop.contains("release_and_stop_process(&self.orch, &self.run_id)"));
    }

    #[test]
    fn the_cli_resume_miss_classifies_as_session_unresolvable() {
        // Verbatim from the runner log that opened CAIRN-3104.
        assert_eq!(
            classify_stderr_failure(
                "No conversation found with session ID: 5c7fe52a-b0bd-4f3b-8bde-d7af9a2b2b93"
            ),
            Some(BackendFailure::SessionUnresolvable)
        );
    }

    #[test]
    fn the_cli_session_id_collision_classifies_as_in_use() {
        // Verbatim from the runner log that opened cairn/4978. Not
        // SessionUnresolvable: the conversation exists, so no reseed is owed.
        assert_eq!(
            classify_stderr_failure(
                "Error: Session ID 30fb0390-c508-4345-b87a-fd0f2f10f1b5 is already in use."
            ),
            Some(BackendFailure::SessionIdInUse)
        );
    }

    #[test]
    fn ordinary_stderr_classifies_as_nothing() {
        assert_eq!(classify_stderr_failure(""), None);
        assert_eq!(
            classify_stderr_failure("(node:12345) ExperimentalWarning: stream/web is experimental"),
            None
        );
        // Merely naming a session is not this failure.
        assert_eq!(
            classify_stderr_failure("Resuming session ID: 5c7fe52a-b0bd-4f3b-8bde-d7af9a2b2b93"),
            None
        );
    }
}

#[cfg(test)]
mod terminal_tool_tests {
    use super::{classify_eof, should_finalize_task_run_on_terminal_tool_eof, EofVerdict};
    use crate::models::RunStatus;

    /// Every status a run row can carry today, spelled as the store writes it.
    fn stored(status: RunStatus) -> String {
        status.to_string()
    }

    const ACTIVE: [RunStatus; 2] = [RunStatus::Starting, RunStatus::Live];
    const TERMINAL: [RunStatus; 2] = [RunStatus::Exited, RunStatus::Crashed];

    #[test]
    fn terminal_tool_task_eof_is_treated_as_completed_while_active() {
        for status in ACTIVE {
            let status = stored(status);
            assert!(should_finalize_task_run_on_terminal_tool_eof(
                true,
                Some(&status),
                true
            ));
            assert!(!should_finalize_task_run_on_terminal_tool_eof(
                false,
                Some(&status),
                true
            ));
            assert!(!should_finalize_task_run_on_terminal_tool_eof(
                true,
                Some(&status),
                false
            ));
        }
        for status in TERMINAL {
            assert!(!should_finalize_task_run_on_terminal_tool_eof(
                true,
                Some(&stored(status)),
                true
            ));
        }
        assert!(!should_finalize_task_run_on_terminal_tool_eof(
            true, None, true
        ));
    }

    #[test]
    fn classify_eof_warm_is_exited() {
        // Host-warmed (interrupt/eviction) closes stdout: completed, not a crash.
        for status in ACTIVE {
            assert_eq!(
                classify_eof(true, false, false, Some(&stored(status)), false, false),
                EofVerdict::Exited
            );
        }
    }

    #[test]
    fn classify_eof_terminal_result_then_eof_is_exited() {
        // A run that emitted a terminal Result and then closed stdout completed,
        // regardless of occupancy — belt-and-suspenders over the warm path.
        for status in ACTIVE {
            assert_eq!(
                classify_eof(false, true, false, Some(&stored(status)), false, false),
                EofVerdict::Exited
            );
        }
    }

    #[test]
    fn classify_eof_terminal_429_remains_interrupted_for_rollover() {
        for status in ACTIVE {
            assert_eq!(
                classify_eof(false, true, true, Some(&stored(status)), false, false),
                EofVerdict::Crashed
            );
        }
    }

    #[test]
    fn classify_eof_no_result_live_not_warm_is_crashed() {
        // No terminal Result on a live run → Crashed, which interrupts the
        // running turn (leaving the job resumable). A blocking rate-limit exit
        // lands here too: the rate-limit signal refines the error message, not
        // the verdict — finalizing Exited would wrongly complete the turn and
        // advance downstream onto work the blocked account never finished.
        assert_eq!(
            classify_eof(false, false, false, Some("live"), false, false),
            EofVerdict::Crashed
        );
    }

    #[test]
    fn classify_eof_no_result_starting_not_warm_is_crashed() {
        // A process that dies before its first event (a refused start) settles
        // on EOF too, rather than waiting for the reaper.
        assert_eq!(
            classify_eof(false, false, false, Some("starting"), false, false),
            EofVerdict::Crashed
        );
    }

    #[test]
    fn classify_eof_task_spawned_terminal_tool_is_exited() {
        for status in ACTIVE {
            assert_eq!(
                classify_eof(false, false, false, Some(&stored(status)), true, true),
                EofVerdict::Exited
            );
        }
    }

    #[test]
    fn classify_eof_already_terminal_when_settled_or_unknown() {
        for status in TERMINAL {
            assert_eq!(
                classify_eof(false, false, false, Some(&stored(status)), false, false),
                EofVerdict::AlreadyTerminal
            );
        }
        assert_eq!(
            classify_eof(false, false, false, None, false, false),
            EofVerdict::AlreadyTerminal
        );
    }
}

/// CAIRN-1611: a fully-emitted assistant message parked for deferred
/// finalization must be committed, not dropped, when a terminal-tool or
/// host-interrupt guard suppresses the in-flight stream.
#[cfg(test)]
mod flush_pending_tests {
    use super::{
        flush_pending_assistant_before_suppress, merge_pending_assistant, ClaudeBackend,
        StreamingState,
    };
    use crate::agent_process::process::{RunHandle, RunOccupancy};
    use crate::agent_process::stream::{ToolUseInfo, TranscriptEvent};
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, RowExt, SearchIndex};
    use crate::transcripts::stream_store::open_stream;
    use cairn_db::turso::params;
    use cairn_models::wire::claude_stream::parse_event;
    use std::sync::Arc;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("flush-pending-test.db"))
            .await
            .unwrap();
        db
    }

    fn build_orch(db: LocalDb) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    async fn insert_run(orch: &Orchestrator, id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        orch.db
            .local
            .write(|conn| {
                let id = id.to_string();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO runs(
                             id, status, session_id, created_at, updated_at
                         )
                         VALUES (?1, 'live', 'session-1', ?2, ?3)",
                        params![id.as_str(), now, now],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    /// Seed the turn a resumed reader's events bind to. `events.turn_id`
    /// references `turns(id)`, so a reader serving a turn writes nothing at all
    /// without this row — every insert fails the foreign key.
    async fn insert_turn(orch: &Orchestrator, run_id: &str, turn_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        orch.db
            .local
            .write(|conn| {
                let run_id = run_id.to_string();
                let turn_id = turn_id.to_string();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO turns(
                             id, session_id, run_id, sequence, state, start_reason,
                             created_at, updated_at
                         )
                         VALUES (?1, 'session-1', ?2, 1, 'running', 'initial', ?3, ?4)",
                        params![turn_id.as_str(), run_id.as_str(), now, now],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    fn write_tool_assistant(tool_use_id: &str) -> TranscriptEvent {
        TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some("session-1".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: Some(vec![ToolUseInfo {
                id: tool_use_id.to_string(),
                name: "mcp__cairn__write".to_string(),
                input: serde_json::json!({ "changes": [] }),
            }]),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        }
    }

    fn read_tool_assistant(tool_use_id: &str) -> TranscriptEvent {
        TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some("session-1".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: Some(vec![ToolUseInfo {
                id: tool_use_id.to_string(),
                name: "mcp__cairn__read".to_string(),
                input: serde_json::json!({ "paths": [] }),
            }]),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        }
    }

    async fn count_events(orch: &Orchestrator, run_id: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM events WHERE run_id = '{run_id}'");
        orch.db
            .local
            .query_one(sql.as_str(), (), |row| row.i64(0))
            .await
            .unwrap()
    }

    async fn count_assistant_with_tooluse(
        orch: &Orchestrator,
        run_id: &str,
        tool_use_id: &str,
    ) -> i64 {
        let sql = format!(
            "SELECT COUNT(*) FROM events \
             WHERE run_id = '{run_id}' AND event_type = 'assistant' \
             AND data LIKE '%{tool_use_id}%'"
        );
        orch.db
            .local
            .query_one(sql.as_str(), (), |row| row.i64(0))
            .await
            .unwrap()
    }

    /// Every persisted sequence for a run, in ascending order.
    async fn sequences(orch: &Orchestrator, run_id: &str) -> Vec<i64> {
        let sql =
            format!("SELECT sequence FROM events WHERE run_id = '{run_id}' ORDER BY sequence ASC");
        orch.db
            .local
            .query_all(sql, (), |row| row.i64(0))
            .await
            .unwrap()
    }

    /// One streamed, tool-using turn in the wire order that produced CAIRN-3290.
    ///
    /// The consolidated `assistant` event arrives BEFORE its trailing
    /// `message_delta` (the order Opus-class models emit), so it parks; the
    /// `tool_result` for its tool call then lands while the stream row is still
    /// open holding a reserved slot. That interleaving is what made the old
    /// counter hand the same number to two events and skip the next.
    fn streamed_tool_turn_fixture() -> String {
        [
            r#"{"type":"system","subtype":"init","session_id":"session-1","cwd":"/tmp","model":"claude-opus-4","tools":["mcp__cairn__read","mcp__cairn__write","mcp__cairn__run"],"mcp_servers":[{"name":"cairn","status":"connected"}]}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"message_start","message":{"id":"msg_01","role":"assistant","model":"claude-opus-4"}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Reading the file."}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_stop","index":0}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01","name":"mcp__cairn__read","input":{}}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"paths\":[\"file:src/lib.rs\"]}"}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_stop","index":1}}"#,
            r#"{"type":"assistant","uuid":"evt-assistant","session_id":"session-1","message":{"role":"assistant","content":[{"type":"text","text":"Reading the file."},{"type":"tool_use","id":"toolu_01","name":"mcp__cairn__read","input":{"paths":["file:src/lib.rs"]}}]}}"#,
            r#"{"type":"user","uuid":"evt-tool-result","session_id":"session-1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"pub fn main() {}"}]}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"input_tokens":11,"output_tokens":22,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"message_stop"}}"#,
            r#"{"type":"result","subtype":"success","session_id":"session-1","is_error":false,"duration_ms":1200,"num_turns":1,"total_cost_usd":0.01,"usage":{"input_tokens":11,"output_tokens":22,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
        ]
        .join("\n")
    }

    /// The fixture must keep exercising the branches it claims to. If the wire
    /// shapes drift, this fails loudly rather than letting the regression test
    /// silently run over a stream of `Unknown`.
    #[test]
    fn fixture_parses_into_the_branches_under_test() {
        let kinds: Vec<&'static str> = streamed_tool_turn_fixture()
            .lines()
            .map(|line| parse_event(line).expect("fixture line parses").0.kind())
            .collect();
        assert!(!kinds.contains(&"unknown"));
        assert_eq!(kinds.first().copied(), Some("system"));
        assert_eq!(kinds.last().copied(), Some("result"));
        assert!(kinds.contains(&"assistant"));
        assert!(kinds.contains(&"user"));
        assert!(kinds.contains(&"stream_event"));
    }

    /// CAIRN-3290: the persisted transcript for one streamed tool turn carries
    /// each sequence exactly once, with no gap.
    #[tokio::test]
    async fn resumed_reader_persists_tool_result_while_serving_successor() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-reader").await;
        insert_turn(&orch, "run-reader", "successor-turn").await;
        orch.process_state
            .register_process(
                "run-reader".to_string(),
                RunHandle::test_handle(Some("session-1"), None),
            )
            .unwrap();
        assert_eq!(
            orch.process_state.get_occupancy("run-reader"),
            Some(RunOccupancy::Idle)
        );
        orch.process_state.transition_to_active("run-reader");
        orch.process_state
            .begin_turn("run-reader", "successor-turn");
        assert_eq!(
            orch.process_state.get_occupancy("run-reader"),
            Some(RunOccupancy::ServingTurn("successor-turn".to_string()))
        );

        let orch_thread = orch.clone();
        let run_db = orch.db.local.clone();
        let emitter = orch.services.emitter.clone();
        let stdout = Box::new(std::io::Cursor::new(
            streamed_tool_turn_fixture().into_bytes(),
        ));
        std::thread::spawn(move || {
            ClaudeBackend::reader_thread(
                &orch_thread,
                &emitter,
                "run-reader",
                Some("session-1".to_string()),
                false,
                stdout,
                None,
                run_db,
            );
        })
        .join()
        .unwrap();

        let sequences = sequences(&orch, "run-reader").await;
        assert_eq!(
            sequences,
            (0..sequences.len() as i64).collect::<Vec<_>>(),
            "sequences must be exactly 0..n — no duplicate, no gap"
        );

        // The turn's four durable events: the init notice, the assistant message
        // that issued the tool call, its result, and the terminal result.
        let types = orch
            .db
            .local
            .query_all(
                "SELECT event_type FROM events WHERE run_id = 'run-reader' ORDER BY sequence ASC",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            types,
            vec![
                "system:init".to_string(),
                "assistant".to_string(),
                "tool_result".to_string(),
                "result:success".to_string(),
            ],
            "the assistant message sorts where it STARTED streaming, ahead of the \
             tool_result that arrived before its trailing message_delta"
        );
        let terminal_turn = orch.db.local.query_opt(
            "SELECT turn_id FROM events WHERE run_id='run-reader' AND event_type='result:success'", (), |row| row.text(0),
        ).await.unwrap().unwrap();
        assert_eq!(
            terminal_turn, "successor-turn",
            "persist the Result before the host clears the current turn"
        );
    }

    const STATED_INTENT: &str = "Reading both files first.";
    const TURN_REASONING_TOKENS: u32 = 412;

    /// One tool-using turn as the CLI streams reasoning. A PRIVATE block (empty
    /// text, signature only) always leads; `visible` adds the stated-intent block
    /// a Fable-class model emits after it. Each block is closed by its own
    /// consolidated `assistant` message, and the turn's reasoning count -- private
    /// and visible together -- arrives last, on `message_delta`. Signatures are
    /// placeholders; a real one must never be committed.
    fn reasoning_turn_fixture(visible: Option<&str>) -> String {
        use serde_json::{json, Value};
        let sid = "session-1";
        let stream =
            |event: Value| json!({"type": "stream_event", "session_id": sid, "event": event});
        let assistant = |uuid: &str, content: Value| {
            json!({"type": "assistant", "uuid": uuid, "session_id": sid,
                   "message": {"role": "assistant", "content": content}})
        };
        let estimate = |tokens: u32| json!({"type": "system", "subtype": "thinking_tokens", "session_id": sid, "estimated_tokens": tokens});
        let signature = |index: usize| {
            stream(json!({"type": "content_block_delta", "index": index,
                          "delta": {"type": "signature_delta", "signature": "<redacted>"}}))
        };
        // The reader fails closed on an init that lacks any core verb, so the
        // fixture declares exactly the set the startup gate requires.
        let mut lines = vec![
            json!({"type": "system", "subtype": "init", "session_id": sid, "cwd": "/tmp",
                   "model": "claude-fable-5",
                   "tools": cairn_session::agent_process::toolkits::CORE_VERBS,
                   "mcp_servers": [{"name": "cairn", "status": "connected"}]}),
            stream(json!({"type": "message_start",
                          "message": {"id": "msg_01", "role": "assistant", "model": "claude-fable-5"}})),
            stream(json!({"type": "content_block_start", "index": 0,
                          "content_block": {"type": "thinking", "thinking": ""}})),
            estimate(120),
            signature(0),
            stream(json!({"type": "content_block_stop", "index": 0})),
            assistant(
                "evt-private",
                json!([{"type": "thinking", "thinking": "", "signature": "<redacted>"}]),
            ),
        ];
        let mut tool_index = 1;
        if let Some(prose) = visible {
            let (head, tail) = prose.split_at(prose.len() / 2);
            lines.extend([
                stream(json!({"type": "content_block_start", "index": 1,
                              "content_block": {"type": "thinking", "thinking": ""}})),
                stream(json!({"type": "content_block_delta", "index": 1,
                              "delta": {"type": "thinking_delta", "thinking": head}})),
                estimate(380),
                stream(json!({"type": "content_block_delta", "index": 1,
                              "delta": {"type": "thinking_delta", "thinking": tail}})),
                signature(1),
                stream(json!({"type": "content_block_stop", "index": 1})),
                assistant(
                    "evt-visible",
                    json!([{"type": "thinking", "thinking": prose, "signature": "<redacted>"}]),
                ),
            ]);
            tool_index = 2;
        }
        lines.extend([
            stream(json!({"type": "content_block_start", "index": tool_index,
                          "content_block": {"type": "tool_use", "id": "toolu_01", "name": "mcp__cairn__read", "input": {}}})),
            stream(json!({"type": "content_block_delta", "index": tool_index,
                          "delta": {"type": "input_json_delta", "partial_json": "{\"paths\":[\"file:notes.txt\"]}"}})),
            stream(json!({"type": "content_block_stop", "index": tool_index})),
            assistant("evt-tool", json!([{"type": "tool_use", "id": "toolu_01", "name": "mcp__cairn__read",
                                          "input": {"paths": ["file:notes.txt"]}}])),
            stream(json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"},
                          "usage": {"input_tokens": 11, "output_tokens": 470,
                                    "output_tokens_details": {"thinking_tokens": TURN_REASONING_TOKENS}}})),
            stream(json!({"type": "message_stop"})),
            json!({"type": "user", "uuid": "evt-tool-result", "session_id": sid,
                   "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_01", "content": "three lines"}]}}),
            json!({"type": "result", "subtype": "success", "session_id": sid, "is_error": false,
                   "duration_ms": 1200, "num_turns": 1, "total_cost_usd": 0.01,
                   "usage": {"input_tokens": 11, "output_tokens": 470}}),
        ]);
        lines
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Drive the real reader over `fixture` for a run serving one live turn.
    async fn replay_reasoning_turn(orch: &Orchestrator, run_id: &str, fixture: String) {
        insert_run(orch, run_id).await;
        insert_turn(orch, run_id, "turn-1").await;
        orch.process_state
            .register_process(
                run_id.to_string(),
                RunHandle::test_handle(Some("session-1"), None),
            )
            .unwrap();
        orch.process_state.transition_to_active(run_id);
        orch.process_state.begin_turn(run_id, "turn-1");
        let orch_thread = orch.clone();
        let run_db = orch.db.local.clone();
        let emitter = orch.services.emitter.clone();
        let run_id_thread = run_id.to_string();
        std::thread::spawn(move || {
            ClaudeBackend::reader_thread(
                &orch_thread,
                &emitter,
                &run_id_thread,
                Some("session-1".to_string()),
                false,
                Box::new(std::io::Cursor::new(fixture.into_bytes())),
                None,
                run_db,
            );
        })
        .join()
        .unwrap();
    }

    /// The persisted event types, and for each assistant row its prose (or
    /// `<none>`) and its reasoning count (or -1), all in sequence order.
    async fn persisted_reasoning(
        orch: &Orchestrator,
        run_id: &str,
    ) -> (Vec<String>, Vec<String>, Vec<i64>) {
        let db = &orch.db.local;
        let types = db
            .query_all(
                format!(
                    "SELECT event_type FROM events WHERE run_id = '{run_id}' ORDER BY sequence"
                ),
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        let prose = db
            .query_all(
                format!(
                    "SELECT COALESCE(json_extract(data, '$.thinking'), '<none>') FROM events \
                     WHERE run_id = '{run_id}' AND event_type = 'assistant' ORDER BY sequence"
                ),
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        let counts = db
            .query_all(
                format!(
                    "SELECT COALESCE(thinking_tokens, -1) FROM events \
                     WHERE run_id = '{run_id}' AND event_type = 'assistant' ORDER BY sequence"
                ),
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        (types, prose, counts)
    }

    /// The text of every `system:error` the reader persisted, so a failed
    /// event-type assertion names why the turn was refused.
    async fn persisted_errors(orch: &Orchestrator, run_id: &str) -> Vec<String> {
        orch.db
            .local
            .query_all(
                format!(
                    "SELECT data FROM events \
                     WHERE run_id = '{run_id}' AND event_type = 'system:error' ORDER BY sequence"
                ),
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
    }

    #[test]
    fn reasoning_fixtures_parse_into_the_branches_under_test() {
        for fixture in [
            reasoning_turn_fixture(Some(STATED_INTENT)),
            reasoning_turn_fixture(None),
        ] {
            let kinds: Vec<&'static str> = fixture
                .lines()
                .map(|line| parse_event(line).expect("fixture line parses").0.kind())
                .collect();
            assert!(!kinds.contains(&"unknown"), "{kinds:?}");
        }
    }

    /// A Fable-class turn states its intent AND reasons privately. The persisted
    /// assistant event must carry both facts in their own fields: the stated
    /// prose exactly, and the turn's whole reasoning count -- with the private
    /// block contributing neither a separator nor a row of its own.
    #[tokio::test]
    async fn a_private_then_visible_reasoning_turn_persists_prose_and_count() {
        let orch = build_orch(test_db().await);
        replay_reasoning_turn(
            &orch,
            "run-fable",
            reasoning_turn_fixture(Some(STATED_INTENT)),
        )
        .await;
        let (types, prose, counts) = persisted_reasoning(&orch, "run-fable").await;
        assert_eq!(
            types,
            ["system:init", "assistant", "tool_result", "result:success"],
            "one assistant event for the turn: no orphan row for either reasoning block; \
             errors: {:?}",
            persisted_errors(&orch, "run-fable").await
        );
        assert_eq!(
            prose,
            [STATED_INTENT],
            "the stated intent, with no leading separator"
        );
        assert_eq!(counts, [TURN_REASONING_TOKENS as i64]);
    }

    /// An Opus-class turn reasons privately only: the count survives, no prose
    /// is invented, and the private block leaves no empty row behind.
    #[tokio::test]
    async fn a_private_only_reasoning_turn_persists_the_count_and_no_prose() {
        let orch = build_orch(test_db().await);
        replay_reasoning_turn(&orch, "run-opus", reasoning_turn_fixture(None)).await;
        let (types, prose, counts) = persisted_reasoning(&orch, "run-opus").await;
        assert_eq!(
            types,
            ["system:init", "assistant", "tool_result", "result:success"],
            "errors: {:?}",
            persisted_errors(&orch, "run-opus").await
        );
        assert_eq!(prose, ["<none>"]);
        assert_eq!(counts, [TURN_REASONING_TOKENS as i64]);
    }

    #[tokio::test]
    async fn flush_commits_pending_assistant_paired_with_stream() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-paired").await;
        let opened = open_stream(
            orch.db.local.clone(),
            "run-paired",
            Some("session-1"),
            None,
            "claude",
        )
        .unwrap();
        let mut streaming_state = Some(StreamingState::new(&opened));
        let mut pending = Some(write_tool_assistant("toolu_paired"));

        flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-paired",
            Some("session-1"),
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert!(pending.is_none(), "pending must be cleared after commit");
        assert!(
            streaming_state.is_none(),
            "stream must be consumed by finalize"
        );
        assert_eq!(
            count_assistant_with_tooluse(&orch, "run-paired", "toolu_paired").await,
            1,
            "the tool-call assistant event must be persisted"
        );
    }

    #[tokio::test]
    async fn flush_inserts_directly_when_stream_already_gone() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-orphan").await;
        let mut streaming_state: Option<StreamingState> = None;
        let mut pending = Some(write_tool_assistant("toolu_orphan"));

        flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-orphan",
            Some("session-1"),
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert!(pending.is_none(), "pending must be cleared after insert");
        assert_eq!(
            count_assistant_with_tooluse(&orch, "run-orphan", "toolu_orphan").await,
            1,
            "the tool-call assistant event must be persisted via direct insert"
        );
    }

    #[tokio::test]
    async fn flush_is_noop_without_pending_assistant() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-empty").await;
        let mut streaming_state: Option<StreamingState> = None;
        let mut pending: Option<TranscriptEvent> = None;

        flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-empty",
            Some("session-1"),
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert_eq!(
            count_events(&orch, "run-empty").await,
            0,
            "no pending message means nothing is written"
        );
    }

    // CAIRN-2249: a batched multi-tool turn arrives as two consolidated
    // assistant events before the trailing message_delta. The park slot must
    // merge them rather than overwrite, so every tool call survives.
    #[test]
    fn merge_appends_disjoint_tool_uses() {
        let mut parked = read_tool_assistant("toolu_read");
        let incoming = write_tool_assistant("toolu_write");
        merge_pending_assistant(&mut parked, &incoming);
        let uses = parked.tool_uses.as_ref().unwrap();
        assert_eq!(uses.len(), 2);
        assert_eq!(uses[0].id, "toolu_read");
        assert_eq!(uses[1].id, "toolu_write");
        assert!(
            parked.tool_name.is_none(),
            "multi-tool event must clear the legacy single-tool name"
        );
    }

    #[test]
    fn merge_dedups_cumulative_tool_uses() {
        // The cumulative shape: incoming repeats the parked `read` then adds
        // `write`. Dedup-by-id must keep a single `read`.
        let mut parked = read_tool_assistant("toolu_read");
        let mut incoming = read_tool_assistant("toolu_read");
        incoming.tool_uses = Some(vec![
            ToolUseInfo {
                id: "toolu_read".to_string(),
                name: "mcp__cairn__read".to_string(),
                input: serde_json::json!({ "paths": [] }),
            },
            ToolUseInfo {
                id: "toolu_write".to_string(),
                name: "mcp__cairn__write".to_string(),
                input: serde_json::json!({ "changes": [] }),
            },
        ]);
        merge_pending_assistant(&mut parked, &incoming);
        let uses = parked.tool_uses.as_ref().unwrap();
        assert_eq!(uses.len(), 2, "duplicate read must not be re-added");
        assert_eq!(uses[0].id, "toolu_read");
        assert_eq!(uses[1].id, "toolu_write");
    }

    #[test]
    fn merge_retains_parked_content_when_incoming_empty() {
        let mut parked = read_tool_assistant("toolu_read");
        parked.content = Some("I'll dig".to_string());
        let incoming = write_tool_assistant("toolu_write");
        merge_pending_assistant(&mut parked, &incoming);
        assert_eq!(parked.content.as_deref(), Some("I'll dig"));
    }

    // A parked message carrying only PRIVATE reasoning has an empty prose slot.
    // `Some("")` is not `None`, so an `is_none()` guard here would treat that
    // nothing as prose and swallow the visible intent arriving behind it.
    #[test]
    fn merge_adopts_incoming_prose_over_an_empty_parked_thinking() {
        let mut parked = read_tool_assistant("toolu_read");
        parked.thinking = Some(String::new());
        let mut incoming = write_tool_assistant("toolu_write");
        incoming.thinking = Some("Checking the second file too.".to_string());
        merge_pending_assistant(&mut parked, &incoming);
        assert_eq!(
            parked.thinking.as_deref(),
            Some("Checking the second file too."),
            "an empty parked thinking value is absence, not prose to protect"
        );
    }

    #[test]
    fn merge_retains_parked_prose_against_a_later_message_without_any() {
        let mut parked = read_tool_assistant("toolu_read");
        parked.thinking = Some("Reading both files first.".to_string());
        let mut incoming = write_tool_assistant("toolu_write");
        incoming.thinking = None;
        merge_pending_assistant(&mut parked, &incoming);
        assert_eq!(
            parked.thinking.as_deref(),
            Some("Reading both files first.")
        );
    }

    #[test]
    fn merge_keeps_single_tool_legacy_fields() {
        let mut parked = read_tool_assistant("toolu_read");
        let incoming = read_tool_assistant("toolu_read");
        merge_pending_assistant(&mut parked, &incoming);
        let uses = parked.tool_uses.as_ref().unwrap();
        assert_eq!(uses.len(), 1, "the repeated read collapses to one");
        assert_eq!(
            parked.tool_name.as_deref(),
            Some("mcp__cairn__read"),
            "a sole surviving tool repopulates the legacy single-tool name"
        );
    }

    // Reader-level regression: parking a `read` event then merging a `write`
    // event (no pending_delta_usage) and finalizing must persist a single
    // assistant event that carries BOTH tool calls.
    #[tokio::test]
    async fn merge_then_flush_persists_all_batched_tool_calls() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-merge").await;
        let opened = open_stream(
            orch.db.local.clone(),
            "run-merge",
            Some("session-1"),
            None,
            "claude",
        )
        .unwrap();
        let mut streaming_state = Some(StreamingState::new(&opened));

        // Simulate the park site for a batched turn: read parked, write merged.
        let mut pending = Some(read_tool_assistant("toolu_read"));
        let write_event = write_tool_assistant("toolu_write");
        match pending.as_mut() {
            Some(parked) => merge_pending_assistant(parked, &write_event),
            None => pending = Some(write_event.clone()),
        }

        flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-merge",
            Some("session-1"),
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert_eq!(
            count_assistant_with_tooluse(&orch, "run-merge", "toolu_read").await,
            1,
            "the read tool call must survive the batched turn"
        );
        assert_eq!(
            count_assistant_with_tooluse(&orch, "run-merge", "toolu_write").await,
            1,
            "the write tool call must survive the batched turn"
        );
    }
}

impl ClaudeBackend {
    #[allow(clippy::too_many_arguments)]
    fn reader_thread(
        orch: &Orchestrator,
        emitter: &Arc<dyn crate::services::EventEmitter>,
        run_id: &str,
        session_id: Option<String>,
        confirm_backend_id_after_init: bool,
        stdout: Box<dyn BufRead + Send>,
        stderr_watch: Option<StderrWatch>,
        run_db: Arc<LocalDb>,
    ) {
        let runtime = claude_runtime(orch, emitter, run_id, session_id.clone(), run_db.clone());
        ClaudeRuntime::reader_thread(
            &runtime,
            run_id,
            session_id,
            confirm_backend_id_after_init,
            stdout,
            stderr_watch,
            run_db,
        );
    }
}
use super::transcript_host::SessionTranscriptHost as ClaudeReaderHost;
impl ClaudeHost for ClaudeReaderHost {
    fn transcripts(&self) -> &dyn cairn_session::transcripts::TranscriptHost {
        self
    }
    fn error(&self, message: &str) {
        insert_error_event(
            &self.orch,
            &self.run_id,
            self.session_id.as_deref(),
            message,
        );
    }
    fn stop_process(&self) {
        crate::orchestrator::lifecycle::release_and_stop_process(&self.orch, &self.run_id);
    }
    fn startup_rejected(&self, message: &str) {
        if settle_active_run_as_crashed(
            &self.orch,
            &self.run_id,
            BackendFailure::StartupRejected.exit_reason(),
            message,
        ) {
            self.error(message);
            crate::orchestrator::lifecycle::finalize_run(
                &self.orch,
                &self.run_id,
                RunStatus::Crashed,
            );
        }
    }
    fn first_output(&self) {
        crate::execution::jobs::spawn_produced_first_output(&self.orch, &self.run_id);
    }
    fn initialized(&self) {
        let orch = &self.orch;
        let run_db = self.run_db.clone();
        let run_id = self.run_id.as_str();
        if let Ok(Some(target)) =
            rate_limit_retry_target(orch, &run_db, run_id, self.session_id.as_deref())
        {
            orch.capture_claude_usage_after_launch(target.account_id);
        }
    }
    fn capture_output(&self, structured: serde_json::Value) {
        let orch = &self.orch;
        let run_id = self.run_id.as_str();
        let orch_capture = orch.clone();
        let run_id_capture = run_id.to_string();
        match run_backend_db(CLAUDE_BACKEND_NAME, async move {
            crate::mcp::handlers::comments_artifacts::capture_call_structured_output(
                &orch_capture,
                &run_id_capture,
                structured,
            )
            .await
        }) {
            Ok(_) => {}
            Err(e) => log::warn!(
                "Failed to capture structured_output for run {}: {}",
                &run_id[..run_id.len().min(8)],
                e
            ),
        }
    }
    fn rate_limit(&self, snapshot: ProviderUsageSnapshot, info: Option<&RateLimitInfo>) -> bool {
        self.orch.store_provider_usage_snapshot(snapshot.clone());
        if info.is_some_and(|info| !info.is_blocking()) {
            return false;
        }
        let Ok(Some(target)) = rate_limit_retry_target(
            &self.orch,
            &self.run_db,
            &self.run_id,
            self.session_id.as_deref(),
        ) else {
            return false;
        };
        let provider_reset = info.and_then(|info| info.resets_at.or(info.overage_resets_at));
        let blocked_until = Some(match provider_reset {
            Some(at) => AccountBlock::Declared(at),
            None => AccountBlock::Reprobe(
                snapshot.captured_at + TERMINAL_429_REPROBE_AFTER.as_secs() as i64,
            ),
        });
        let health_windows = if provider_reset.is_some() {
            snapshot.windows
        } else {
            Vec::new()
        };
        match self
            .orch
            .record_account_health(&target.account_id, health_windows, blocked_until)
        {
            Ok(_) => true,
            Err(error) => {
                log::warn!("Failed to record Claude account block: {error}");
                false
            }
        }
    }
    fn provider_limit_result(&self, report: ProviderLimitReport<'_>) -> bool {
        match hold_claude_provider_limit(self, report) {
            Ok(quarantined) => quarantined,
            Err(error) => {
                // Neither the yield nor its hold landed. Settle the turn the
                // way an exhausted account does: unfinished and resumable on an
                // explicit continuation, never a completion and never an
                // unowned yield.
                crate::orchestrator::lifecycle::interrupt_run_for_provider_depletion(
                    &self.orch,
                    &self.run_id,
                );
                self.error(&format!(
                    "Could not persist the provider limit hold: {error}. Work remains unfinished."
                ));
                false
            }
        }
    }
    fn context_snapshot(&self, snapshot: ContextTokenState) {
        self.orch.store_context_token_snapshot(snapshot);
    }
    fn complete(&self, outcome: ClaudeTurnOutcome, context: ClaudeTurnContext) {
        apply_claude_turn_completion(
            &self.orch,
            &self.run_db,
            &self.emitter,
            &self.run_id,
            self.session_id.as_deref(),
            outcome,
            context,
        );
    }
    fn artifact_handoff(&self) {
        apply_claude_artifact_handoff(&self.orch, &self.emitter, &self.run_id);
    }
    fn interrupt(&self) -> Result<(), String> {
        crate::backends::stdin::send_interrupt(
            &self.orch.process_state,
            &self.run_id,
            crate::agent_process::process::PauseIntent::Reassert,
        )
    }
    fn credential_rejected(&self) {
        // The reader keeps reading while the renewal runs.
        let orch = self.orch.clone();
        let run_id = self.run_id.clone();
        thread::spawn(move || orch.claude_credential_rejected(&run_id));
    }
    fn credential_acknowledged(&self, generation: u64) {
        self.orch
            .claude_credential_acknowledged(&self.run_id, generation);
    }
    fn recover_watchdog(
        &self,
        identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
        now: i64,
    ) -> Result<(), String> {
        crate::orchestrator::lifecycle::recover_provider_watchdog(&self.orch, identity, now)
            .map(|_| ())
    }

    fn eof(&self, report: ClaudeEof) {
        let ClaudeEof {
            evidence,
            persisted_rate_limit_quarantine,
            pending_depletion,
            depletion_handled,
            turn_block_recorded,
            blocking_reset,
            tool_in_flight,
            terminal_tool_called,
            backend_failure,
        } = report;
        let StreamEvidence {
            saw_terminal_result,
            saw_rate_limit_event,
            saw_blocking_rate_limit,
            last_event_kind,
        } = evidence;
        let orch = &self.orch;
        let run_db = self.run_db.clone();
        let run_id = self.run_id.as_str();
        let session_id = self.session_id.clone();
        // Stdout closed - process has terminated.
        //
        // A genuine self-crash still has its RunHandle in the registry at this
        // point — the reader thread itself removes it below, only after this
        // classification. So if the handle is already gone, the orchestrator
        // removed it via an intentional kill (warm-process eviction, user stop,
        // model-change restart). That is a host-initiated termination, not a
        // crash: finalizing it as `Exited` leaves a suspended (Yielded) turn
        // intact so a delegated-wait parent stays resumable, instead of being
        // marked failed and cascading failure to downstream nodes.
        let was_warm = was_host_interrupted(orch, run_id)
            || orch.process_state.get_occupancy(run_id).is_none();
        let run_status_val = run_status(CLAUDE_BACKEND_NAME, &run_db, run_id);

        let task_spawned = is_task_spawned_run(CLAUDE_BACKEND_NAME, &run_db, run_id);
        // The reader has settled stderr before reporting EOF, so every arm sees
        // any diagnosis printed while the process was exiting.

        match classify_eof(
            was_warm,
            saw_terminal_result,
            saw_blocking_rate_limit,
            run_status_val.as_deref(),
            terminal_tool_called,
            task_spawned,
        ) {
            EofVerdict::Exited => {
                log::info!(
                    "Process {} reached EOF in a completed state, finalizing as Exited",
                    &run_id[..run_id.len().min(8)]
                );
                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
            }
            EofVerdict::Crashed => {
                // Structured diagnostic: the next 04:22-style failure must be
                // attributable to a specific job/turn/execution and to whether a
                // rate-limit event preceded the exit. (Child exit status is not
                // available at stdout-EOF in the reader thread; run_status +
                // last_event are the actionable signals here.) `finalize_run`
                // interrupts the still-running turn rather than failing it, so the
                // job stays resumable: a blocking rate-limit exit recovers on
                // reset, and a genuine crash recovers via a follow-up message.
                let (job_id, execution_id) = run_job_execution(&run_db, run_id);
                let turn_id = orch.process_state.get_current_turn_id(run_id);
                log::warn!(
                    "Claude process EOF without terminal result — finalizing Crashed (running turn interrupted, resumable). \
                     run_id={run_id} job_id={job_id:?} execution_id={execution_id:?} \
                     turn_id={turn_id:?} last_event={last_event_kind} \
                     saw_rate_limit_event={saw_rate_limit_event} \
                     saw_blocking_rate_limit={saw_blocking_rate_limit} run_status={run_status_val:?}"
                );

                // A blocking limit whose turn never reached a terminal result is
                // the same refusal a terminal 429 is, and holds the same way.
                // The process is gone, so the held run settles as an exit: the
                // provider-yielded turn stays owned by its hold.
                let mut persisted_rate_limit_quarantine = persisted_rate_limit_quarantine;
                if saw_blocking_rate_limit && !depletion_handled {
                    let held = hold_claude_provider_limit(
                        self,
                        ProviderLimitReport {
                            result_text: None,
                            depletion: pending_depletion
                                .unwrap_or(ProviderDepletion::RollingWindow { resets_at: None }),
                            block_recorded: turn_block_recorded,
                            blocking_reset,
                            tool_in_flight,
                            now: chrono::Utc::now().timestamp(),
                        },
                    );
                    match held {
                        Ok(_) => {
                            log::info!(
                                "Claude process {} exited at a provider limit; the turn is held",
                                &run_id[..run_id.len().min(8)]
                            );
                            crate::orchestrator::lifecycle::finalize_run(
                                orch,
                                run_id,
                                RunStatus::Exited,
                            );
                            return;
                        }
                        Err(error) => {
                            log::warn!("Could not hold the provider-limited run {run_id}: {error}");
                            persisted_rate_limit_quarantine |= turn_block_recorded;
                        }
                    }
                }

                let error_message = if saw_blocking_rate_limit && persisted_rate_limit_quarantine {
                    "Process exited after the account rate limit was reached, before completing the turn. The exhausted profile was quarantined; continue once provider capacity is available to resume safely."
                } else if saw_blocking_rate_limit {
                    "Process exited after the account rate limit was reached, but Cairn could not persist the account quarantine. The turn remains interrupted on its existing profile; retry after provider capacity returns."
                } else if backend_failure == Some(BackendFailure::SessionUnresolvable) {
                    // Deliberately descriptive rather than promissory: the
                    // digest-reseed fallback in `finalize_run` may decline (an
                    // already-attempted session, an active head turn), so this
                    // text must not guarantee a recovery.
                    "The provider could not resolve this session's conversation handle, so the resume produced no output. Cairn's own transcript is intact."
                } else if backend_failure == Some(BackendFailure::SessionIdInUse) {
                    "Claude refused to start this session because an earlier, unconfirmed launch already created it under this id. Cairn's own transcript is intact."
                } else {
                    "Process terminated unexpectedly without completing"
                };

                // Record the typed diagnosis on the run before finalizing, so
                // the lifecycle reaction reads a durable fact off `runs` rather
                // than re-parsing provider text. The write is guarded on the
                // run still being active: a run another path settled between
                // the status read above and here keeps that path's cause, and
                // is not finalized a second time.
                let crash_reason = TurnEndReason::Crash.to_string();
                let exit_reason = backend_failure
                    .map(|failure| failure.exit_reason())
                    .unwrap_or(crash_reason.as_str());
                if settle_active_run_as_crashed(orch, run_id, exit_reason, error_message) {
                    insert_error_event(orch, run_id, session_id.as_deref(), error_message);
                    crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Crashed);
                }
            }
            EofVerdict::AlreadyTerminal => {}
        }
    }
}

/// Claim a still-active run's crash cause. True when this caller owns the
/// settle; a failed write is logged and treated as owning it, so a database
/// hiccup never leaves a dead process's run live for the reaper.
fn settle_active_run_as_crashed(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
    error_message: &str,
) -> bool {
    match crate::orchestrator::lifecycle::record_run_failure_if_active(
        orch,
        run_id,
        exit_reason,
        error_message,
    ) {
        Ok(owned) => owned,
        Err(error) => {
            log::warn!("Failed to record exit reason for run {run_id}: {error}");
            true
        }
    }
}

pub fn claude_runtime(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<String>,
    run_db: Arc<LocalDb>,
) -> ClaudeRuntime {
    ClaudeRuntime {
        process_state: orch.process_state.clone(),
        emitter: emitter.clone(),
        boot_id: orch.boot_at.to_string(),
        host: Arc::new(ClaudeReaderHost {
            orch: orch.clone(),
            emitter: emitter.clone(),
            run_id: run_id.to_string(),
            session_id: session_id.clone(),
            run_db: run_db.clone(),
        }),
    }
}
#[cfg(test)]
fn flush_pending_assistant_before_suppress(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    state: &mut Option<StreamingState>,
    pending: &mut Option<TranscriptEvent>,
    usage: Option<&cairn_models::wire::claude_stream::Usage>,
) {
    let runtime = claude_runtime(
        orch,
        &orch.services.emitter,
        run_id,
        session_id.map(str::to_string),
        db.clone(),
    );
    cairn_session::backends::claude::flush_pending_assistant_before_suppress(
        &runtime, db, run_id, session_id, state, pending, usage,
    );
}
