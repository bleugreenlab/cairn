//! A process Cairn started asks for a one-time ticket to the dev-instance launch
//! route. The ticket book itself lives with the launch, in
//! `crate::dev_instances::launch_ticket`.
//!
//! A run under a `Deny` fence gets none: launching a dev instance starts a runtime
//! outside that fence. With no run behind the request, only a running terminal
//! its executor's relay vouches for gets one. The machine credential's allowlist
//! does not admit this write, and a run-less process that is not a terminal has no
//! use for it. That terminal answers to the run that created it, so a `Deny` fence
//! on that run refuses it too: otherwise a run could get by creating a project
//! terminal what it was refused directly.

use super::super::{build_failure, ResourceMutationResult};
use crate::dev_instances::launch_ticket::{self, Issuer, TerminalIssuer};
use crate::mcp::handlers::fence::{resolve_run_fence, RunFenceResolution};
use crate::mcp::handlers::permission::fence_policy_for_run;
use crate::mcp::handlers::terminal::run_less_terminal_for_session;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::models::Fence;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

const DENY_REFUSAL: &str = "fence denies crossings, and a dev instance's runtime runs outside it";

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    let (CairnResource::DevLaunchTicket, ChangeMode::Append) = (resource, item.mode) else {
        return Ok(None);
    };
    let issuer = match resolve_run_fence(orch, request).await {
        RunFenceResolution::Ambient => Issuer::Terminal(
            asking_terminal(orch, request)
                .await
                .map_err(|refusal| build_failure(index, item, refusal))?,
        ),
        RunFenceResolution::Unavailable(error) => {
            return Err(build_failure(index, item, error));
        }
        RunFenceResolution::Resolved(_, Fence::Deny) => {
            return Err(build_failure(
                index,
                item,
                format!("Refused: this run's {DENY_REFUSAL}"),
            ));
        }
        RunFenceResolution::Resolved(run_id, _) => Issuer::Run(run_id),
    };
    if dry_run {
        return Ok(Some(
            "Would issue a one-time dev-instance launch ticket".to_string(),
        ));
    }
    let ticket = launch_ticket::issue(issuer);
    let summary = format!(
        "Issued a one-time dev-instance launch ticket: {ticket}. It admits one connection to the launch route within a minute."
    );
    *applied_data = Some(serde_json::json!({ "ticket": ticket }));
    Ok(Some(summary))
}

/// The run-less terminal this request came from, if it may have a ticket.
async fn asking_terminal(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<TerminalIssuer, String> {
    let process = request.relayed_run_less_process().ok_or_else(|| {
        "Refused: a launch ticket goes to a run, or to a terminal Cairn started".to_string()
    })?;
    let terminal = run_less_terminal_for_session(&orch.db.local, &process.process_key)
        .await?
        .ok_or_else(|| {
            "Refused: this process is not a running terminal, and a launch ticket goes to a run \
             or to a terminal Cairn started"
                .to_string()
        })?;
    if let Some(creator) = terminal.created_by_run_id.as_deref() {
        match fence_policy_for_run(orch, creator).await {
            Ok(Fence::Deny) => {
                return Err(format!(
                    "Refused: run {creator} created this terminal, and that run's {DENY_REFUSAL}"
                ));
            }
            Ok(_) => {}
            Err(reason) => {
                return Err(format!(
                    "Refused: run {creator} created this terminal, and its fence could not be \
                     read: {reason}"
                ));
            }
        }
    }
    Ok(TerminalIssuer {
        relay_id: process.relay_id.clone(),
        uri: terminal.uri,
        created_by_run_id: terminal.created_by_run_id,
    })
}

#[cfg(test)]
mod tests {
    use super::super::dispatch_resource_change;
    use super::*;
    use crate::db::DbState;
    use crate::fleet::resident_relay::{RegisteredAuthority, ResidentRelayCall};
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_common::executor_protocol::{RelayAuthorization, ResidencyHolder, ResidentRelayId};
    use std::sync::Arc;

    const TICKET: &str = "cairn://dev/launch-ticket";
    const TERMINAL_URI: &str = "cairn://p/cairn/terminal/session";

    async fn orchestrator() -> Orchestrator {
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        crate::project_services::testing::seed_project(&local).await;
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    fn holder() -> ResidencyHolder {
        ResidencyHolder::Job {
            job_id: "terminal-holder".into(),
        }
    }

    /// A project terminal's row, as its create wrote it.
    async fn terminal_row(orch: &Orchestrator, session: &str, status: &str, creator: Option<&str>) {
        orch.db
            .local
            .execute(
                "INSERT INTO job_terminals
                   (id, project_id, session_id, command, status, created_at, slug, created_by_run_id)
                 VALUES (?1, 'p', ?1, 'bun dev:instance', ?2, 1, ?1, ?3)",
                (session, status, creator),
            )
            .await
            .unwrap();
    }

    /// A ticket request as the relay delivers it from the process at `session`,
    /// which was started for no run.
    fn relayed_request(
        orch: &Orchestrator,
        session: &str,
    ) -> (McpCallbackRequest, ResidentRelayId) {
        let mut relays = orch.fleet.resident_relays();
        let holder = holder();
        let minted = relays.register(
            "machine",
            &holder,
            session,
            RegisteredAuthority::Run { run_id: None },
        );
        let relay_id = minted.grant.relay_id;
        relays.bind(&relay_id, "machine", 1).unwrap();
        let mut request = McpCallbackRequest {
            tool: "write".into(),
            payload: serde_json::json!({
                "changes": [{"target": TICKET, "mode": "append", "payload": {}}]
            }),
            ..Default::default()
        };
        relays
            .authorize(
                "machine",
                1,
                &ResidentRelayCall {
                    relay_id: &relay_id,
                    holder: &holder,
                    process_key: session,
                    process_generation: 1,
                    authorization: &RelayAuthorization(format!(
                        "Bearer {}",
                        minted.capability.unwrap()
                    )),
                },
                &mut request,
                &|_, _| Err("no services here".to_string()),
            )
            .unwrap();
        (request, relay_id)
    }

    fn ticket_item() -> ChangeItem {
        ChangeItem {
            target: TICKET.to_string(),
            mode: ChangeMode::Append,
            payload: Some(serde_json::json!({})),
        }
    }

    async fn ask(orch: &Orchestrator, request: &McpCallbackRequest) -> Result<String, String> {
        match dispatch_resource_change(orch, request, 0, &ticket_item(), false).await {
            Ok(change) => {
                let ticket = change.data.unwrap()["ticket"].as_str().unwrap().to_string();
                assert!(change.summary.contains(&ticket), "{}", change.summary);
                Ok(ticket)
            }
            Err(refusal) => Err(format!("{refusal:?}")),
        }
    }

    /// A run named `id` whose fence is `fence`, in its own execution.
    async fn run_under_fence(orch: &Orchestrator, id: &str, number: i64, fence: Fence) {
        let snapshot = serde_json::json!({
            "recipe": {"id": "rec", "name": "Recipe", "description": null, "trigger": "manual",
                       "nodes": [], "edges": []},
            "agents": {"build": {"id": "build", "name": "Build", "description": "Build",
                                 "prompt": "prompt", "tools": [], "disallowedTools": null,
                                 "skills": null, "fence": fence}},
            "skills": {},
            "triggerContext": {"issueId": id, "projectId": "p", "triggerType": "manual"},
            "delegatedPackets": [],
            "createdAt": 1
        })
        .to_string();
        let db = &orch.db.local;
        db.execute(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES(?1, 'p', ?2, 'I', 'active', 'active', 'none', 1, 1)",
            (id, number),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
             VALUES(?1, 'rec', ?1, 'p', 'running', 1, 1, ?2)",
            (id, snapshot.as_str()),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, agent_config_id, created_at, updated_at)
             VALUES(?1, 'p', ?1, ?1, 'builder', 'running', 'build', 1, 1)",
            (id,),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
             VALUES(?1, ?1, 'p', ?1, 'running', 1, 1)",
            (id,),
        )
        .await
        .unwrap();
    }

    /// A terminal no run created gets a ticket naming it, spendable while it
    /// runs and dead the moment it stops, however much of its minute is left.
    #[tokio::test]
    async fn a_run_less_terminals_ticket_names_it_and_dies_with_it() {
        let orch = orchestrator().await;
        terminal_row(&orch, "session", "running", None).await;
        let (request, relay_id) = relayed_request(&orch, "session");

        let ticket = ask(&orch, &request).await.unwrap();
        let issuer = launch_ticket::admit(&orch, &ticket).unwrap();
        assert_eq!(
            issuer,
            Issuer::Terminal(TerminalIssuer {
                relay_id,
                uri: TERMINAL_URI.to_string(),
                created_by_run_id: None,
            })
        );

        let ticket = ask(&orch, &request).await.unwrap();
        orch.fleet
            .resident_relays()
            .revoke_process(&holder(), "session");
        let refusal = launch_ticket::admit(&orch, &ticket).unwrap_err();
        assert!(
            refusal.contains(TERMINAL_URI) && refusal.contains("has ended"),
            "{refusal}"
        );
    }

    /// Only a running terminal the relay vouches for gets a ticket with no run
    /// behind it.
    #[tokio::test]
    async fn a_run_less_caller_that_is_no_running_terminal_gets_no_ticket() {
        let orch = orchestrator().await;
        let refusal = ask(&orch, &McpCallbackRequest::default())
            .await
            .unwrap_err();
        assert!(refusal.contains("Refused"), "{refusal}");

        let (repl, _) = relayed_request(&orch, "repl:scratch");
        let refusal = ask(&orch, &repl).await.unwrap_err();
        assert!(refusal.contains("not a running terminal"), "{refusal}");

        terminal_row(&orch, "gone", "exited", None).await;
        let (exited, _) = relayed_request(&orch, "gone");
        let refusal = ask(&orch, &exited).await.unwrap_err();
        assert!(refusal.contains("not a running terminal"), "{refusal}");
    }

    /// The terminal answers to the run that created it: its ticket names that
    /// run, and a run refused a ticket directly gets none through a terminal.
    #[tokio::test]
    async fn a_terminal_answers_to_the_run_that_created_it() {
        let orch = orchestrator().await;
        run_under_fence(&orch, "run-ask", 1, Fence::Ask).await;
        run_under_fence(&orch, "run-deny", 2, Fence::Deny).await;

        terminal_row(&orch, "by-ask", "running", Some("run-ask")).await;
        let (request, _) = relayed_request(&orch, "by-ask");
        let ticket = ask(&orch, &request).await.unwrap();
        let Issuer::Terminal(issuer) = launch_ticket::admit(&orch, &ticket).unwrap() else {
            panic!("issued to the terminal");
        };
        assert_eq!(issuer.created_by_run_id.as_deref(), Some("run-ask"));

        terminal_row(&orch, "by-deny", "running", Some("run-deny")).await;
        let (request, _) = relayed_request(&orch, "by-deny");
        let refusal = ask(&orch, &request).await.unwrap_err();
        assert!(
            refusal.contains("run-deny") && refusal.contains("denies crossings"),
            "{refusal}"
        );

        terminal_row(&orch, "by-nobody", "running", Some("run-unknown")).await;
        let (request, _) = relayed_request(&orch, "by-nobody");
        let refusal = ask(&orch, &request).await.unwrap_err();
        assert!(refusal.contains("could not be"), "{refusal}");
    }

    /// What the journal says for a launch from a run-less terminal: the terminal
    /// in every case, and its creator when a run created it.
    #[tokio::test]
    async fn a_terminal_launch_is_journaled_against_the_terminal_and_its_creator() {
        let orch = orchestrator().await;
        let issuer = |creator: Option<&str>| {
            Issuer::Terminal(TerminalIssuer {
                relay_id: ResidentRelayId("relay".into()),
                uri: TERMINAL_URI.to_string(),
                created_by_run_id: creator.map(str::to_string),
            })
        };
        for (creator, says) in [
            (Some("run-1"), "created by run run-1"),
            (None, "with no run recorded as its creator"),
        ] {
            crate::authorization::journal_dev_instance_launch(&orch, Some(&issuer(creator))).await;
            let events = cairn_db::storage::authority::list_events(&orch.db.local, "default", 1)
                .await
                .unwrap();
            let event = &events[0];
            assert_eq!(event.request_uri.as_deref(), Some(TERMINAL_URI));
            assert_eq!(event.run_id.as_deref(), creator);
            assert!(
                event.summary.contains(TERMINAL_URI) && event.summary.contains(says),
                "{}",
                event.summary
            );
        }
    }
}
