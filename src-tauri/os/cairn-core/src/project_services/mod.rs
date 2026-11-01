//! Services: long-running processes supervised by the runner and reachable
//! from pages only through the bridge's `service` verb. A project declares one
//! in its repository (`cairn://p/<project>/services/<id>`); a thread writes
//! one like a page (`cairn://p/<project>/<thread>/services/<name>`). The design
//! is in `docs/services.md`.
//!
//! Named `project_services` because `crate::services` is the dependency
//! injection module.

pub(crate) mod authority;
pub(crate) mod declaration;
pub(crate) mod host;
pub(crate) mod log;
pub(crate) mod protocol;
pub(crate) mod rows;
pub(crate) mod schedule;
pub(crate) mod supervisor;
pub(crate) mod thread_store;

use crate::orchestrator::Orchestrator;

/// `jobs.agent_config_id` of a service's job.
pub(crate) const SERVICE_AGENT_CONFIG: &str = "service";
/// `runs.start_mode` of every service run.
pub(crate) const SERVICE_START_MODE: &str = "service";

/// Who owns a service. The owner decides where its packages come from, what
/// its home is, and who may write it and call it; everything else about a
/// service (the supervisor, the protocol, the delegated authority of a call)
/// is the same for both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServiceOwner {
    /// Declared in the project's repository, `.cairn/services/<id>/` at the
    /// base-branch tip.
    Project,
    /// Written by one thread at `cairn:~/services/<name>`. `id` keys every
    /// record the service has; `name` is the thread's address, for its home.
    Thread { id: String, name: String },
}

impl ServiceOwner {
    pub(crate) fn thread_id(&self) -> Option<&str> {
        match self {
            ServiceOwner::Project => None,
            ServiceOwner::Thread { id, .. } => Some(id),
        }
    }

    /// The owner as one stored value: the thread's id, or empty for the
    /// project (`service_log.owner_thread`, and the supervisor's cell key).
    pub(crate) fn key(&self) -> &str {
        self.thread_id().unwrap_or("")
    }

    /// The home a call from the owner itself acts for: the project, or the
    /// owning thread. A scheduled call's `actingFor`.
    pub(crate) fn acting_home(&self, project_key: &str) -> String {
        match self {
            ServiceOwner::Project => cairn_common::uri::build_project_uri(project_key),
            ServiceOwner::Thread { name, .. } => {
                cairn_common::uri::build_thread_uri(project_key, name)
            }
        }
    }

    /// The service's home, which is also its resource.
    pub(crate) fn home(&self, project_key: &str, service: &str) -> String {
        match self {
            ServiceOwner::Project => {
                cairn_common::uri::build_project_service_uri(project_key, service)
            }
            ServiceOwner::Thread { name, .. } => {
                cairn_common::uri::build_thread_service_uri(project_key, name, service)
            }
        }
    }
}

/// The runner's project-services state, held on the orchestrator.
#[derive(Default)]
pub(crate) struct ProjectServices {
    pub(crate) runs: authority::ServiceRuns,
    pub(crate) supervisor: supervisor::ServiceSupervisor,
}

impl std::fmt::Debug for ProjectServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectServices")
            .field("runs", &self.runs)
            .finish_non_exhaustive()
    }
}

/// What confinement does and does not cover for a service process today.
/// Shown in `/status` and repeated in `docs/services.md`; a service is never
/// described as confined without it.
pub(crate) const CONFINEMENT_NOTE: &str = "Confinement: the process runs under the executor's \
     confined sandbox with a write fence, and its only channel to Cairn is its own stdio pipe \
     (no listening endpoint). Not covered today: on Linux, filesystem reads are not enforced \
     (cairn/5492) and pathname unix sockets are reachable below Landlock ABI 9 (cairn/5505); on \
     macOS, outbound unix sockets and XPC are not blocked (cairn/5493). The measured edges for \
     each machine are at cairn://executors.";

/// The project id for a project key.
pub(crate) async fn project_id_for_key(orch: &Orchestrator, key: &str) -> Result<String, String> {
    let key = cairn_common::uri::canonical_project(key);
    orch.db
        .local
        .query_text(
            "SELECT id FROM projects WHERE key = ?1 LIMIT 1",
            (key.clone(),),
        )
        .await
        .map_err(|error| format!("Failed to load project: {error}"))?
        .ok_or_else(|| format!("No project found with key '{key}'"))
}

/// Record a refused tool call in the service's log.
pub(crate) async fn note_refusal(
    orch: &Orchestrator,
    run: &authority::ServiceRun,
    tool: &str,
    refusal: &str,
) {
    let Ok(db) = crate::execution::routing::owning_db_for_project(&orch.db, &run.project_id).await
    else {
        return;
    };
    let policy = crate::security::redaction_policy(&orch.config_dir);
    let role = match &run.role {
        authority::ServiceRunRole::Standing => serde_json::json!({"role": "standing"}),
        authority::ServiceRunRole::Delegated {
            caller_kind,
            acting_for,
            scope,
        } => serde_json::json!({
            "role": "delegated",
            "callerKind": caller_kind.as_str(),
            "actingFor": acting_for,
            "scope": match scope {
                authority::DelegatedScope::ProjectAgent { .. } => "project_agent",
                authority::DelegatedScope::Owner { .. } => "owner",
                authority::DelegatedScope::PageCall { .. } => "page_call",
                authority::DelegatedScope::Empty { .. } => "empty",
            },
        }),
    };
    let entry = log::LogEntry {
        project_id: &run.project_id,
        owner_thread: run.owner.key(),
        service: &run.service,
        generation: run.generation,
        kind: log::LogKind::Lifecycle,
        text: &format!("refused `{tool}`: {refusal}"),
        detail: serde_json::json!({"event": "refused", "tool": tool, "run": role}),
    };
    if let Err(error) = log::record(&db, &policy, &entry).await {
        tracing::warn!(%error, service = %run.service, "could not log a refused service call");
    }
}

/// Record a tool target a call invoked (an MCP tool, a skill script, an
/// executor verb) in the service's log: every one the owner scope admits is
/// a row naming it.
pub(crate) async fn note_target(orch: &Orchestrator, run: &authority::ServiceRun, target: &str) {
    let Ok(db) = crate::execution::routing::owning_db_for_project(&orch.db, &run.project_id).await
    else {
        return;
    };
    let policy = crate::security::redaction_policy(&orch.config_dir);
    let acting_for = match &run.role {
        authority::ServiceRunRole::Delegated { acting_for, .. } => acting_for.as_str(),
        authority::ServiceRunRole::Standing => "",
    };
    let entry = log::LogEntry {
        project_id: &run.project_id,
        owner_thread: run.owner.key(),
        service: &run.service,
        generation: run.generation,
        kind: log::LogKind::Call,
        text: &format!("target `{target}` for {acting_for}"),
        detail: serde_json::json!({"event": "target", "target": target, "actingFor": acting_for}),
    };
    if let Err(error) = log::record(&db, &policy, &entry).await {
        tracing::warn!(%error, service = %run.service, "could not log a service's tool target");
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use crate::storage::LocalDb;

    /// Project `p`, key `cairn`, in the migrated default workspace.
    pub(crate) async fn seed_project(db: &LocalDb) {
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('p', 'default', 'Cairn', 'cairn', '/tmp/cairn', 'main', 1, 1)",
            (),
        )
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod thread_db_tests {
    use super::*;
    use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
    use serde_json::json;

    async fn change(
        orch: &Orchestrator,
        run_id: &str,
        target: &str,
        mode: ChangeMode,
        payload: serde_json::Value,
    ) -> Result<String, String> {
        let request = McpCallbackRequest {
            run_id: Some(run_id.to_string()),
            tool: "write".into(),
            ..Default::default()
        };
        let item = ChangeItem {
            target: target.to_string(),
            mode,
            payload: Some(payload),
        };
        crate::resources::mutations::dispatch_resource_change(orch, &request, 0, &item, false)
            .await
            .map(|applied| applied.summary)
            .map_err(|failure| failure.error)
    }

    /// A page's call to a service lands in the page's thread database, through
    /// the store's own ownership rule; another thread's database stays shut.
    #[tokio::test]
    async fn a_service_call_writes_the_database_of_the_thread_it_acts_for() {
        let (orch, config) = crate::fleet::service_placement::tests::attached_orchestrator().await;
        let db = orch.db.local.clone();
        testing::seed_project(&db).await;
        for name in ["prep", "other"] {
            let thread = crate::threads::crud::create(
                &db,
                crate::models::CreateThread {
                    project_id: "p".into(),
                    name: Some(name.into()),
                    jurisdiction: None,
                    definition: None,
                    migrated_from_number: None,
                    model: None,
                },
            )
            .await
            .unwrap();
            crate::threads::db_store::use_test_path(
                &thread.id,
                config.path().join(format!("{name}.db")),
            );
        }
        let job = rows::ensure_service_job(&db, "p", &ServiceOwner::Project, "judge")
            .await
            .unwrap();
        let register = |role: authority::ServiceRunRole| {
            let (db, job, orch) = (db.clone(), job.clone(), orch.clone());
            async move {
                let run_id = rows::mint_run(&db, "p", &job).await.unwrap();
                orch.project_services.runs.insert(
                    &run_id,
                    authority::ServiceRun {
                        project_id: "p".into(),
                        project_key: "cairn".into(),
                        owner: ServiceOwner::Project,
                        service: "judge".into(),
                        generation: 1,
                        default_branch: "main".into(),
                        role,
                    },
                );
                run_id
            }
        };
        let delegated = |kind, acting_for: &str| authority::ServiceRunRole::Delegated {
            caller_kind: kind,
            acting_for: acting_for.into(),
            scope: authority::delegation_for(
                &ServiceOwner::Project,
                &protocol::CallerFrame {
                    kind,
                    acting_for: acting_for.into(),
                    agent: None,
                    viewer: None,
                    instrument: None,
                },
            ),
        };
        let run_id = register(delegated(
            protocol::CallerKind::Page,
            "cairn://p/cairn/prep",
        ))
        .await;

        change(
            &orch,
            &run_id,
            "cairn://p/cairn/prep/db",
            ChangeMode::Patch,
            json!({"sql": "CREATE TABLE attempts (id INTEGER PRIMARY KEY, passed INTEGER)"}),
        )
        .await
        .expect("the call creates a table in its thread's database");
        change(
            &orch,
            &run_id,
            "cairn://p/cairn/prep/db/attempts",
            ChangeMode::Append,
            json!({"passed": 1}),
        )
        .await
        .expect("the call writes a row in its thread's database");

        let refused = change(
            &orch,
            &run_id,
            "cairn://p/cairn/other/db",
            ChangeMode::Patch,
            json!({"sql": "CREATE TABLE stolen (id INTEGER PRIMARY KEY)"}),
        )
        .await
        .unwrap_err();
        assert!(
            refused.contains("writable only by that thread"),
            "{refused}"
        );

        // Once the call ends, its run writes nothing.
        orch.project_services.runs.remove(&run_id);
        assert!(change(
            &orch,
            &run_id,
            "cairn://p/cairn/prep/db/attempts",
            ChangeMode::Append,
            json!({"passed": 0}),
        )
        .await
        .is_err());

        // An agent acting in a thread (here one of its tasks) writes that
        // thread's database and no other.
        let agent = register(delegated(
            protocol::CallerKind::Agent,
            "cairn://p/cairn/prep/task/scout",
        ))
        .await;
        change(
            &orch,
            &agent,
            "cairn://p/cairn/prep/db/attempts",
            ChangeMode::Append,
            json!({"passed": 0}),
        )
        .await
        .expect("an agent-thread caller's call writes its thread's database");
        assert!(change(
            &orch,
            &agent,
            "cairn://p/cairn/other/db",
            ChangeMode::Patch,
            json!({"sql": "CREATE TABLE x (id INTEGER PRIMARY KEY)"}),
        )
        .await
        .is_err());

        // A draft page (an issue node's) has no owning thread, an issue-node
        // agent has none either, and the service's own standing run acts for
        // no thread at all.
        for role in [
            delegated(protocol::CallerKind::Page, "cairn://p/cairn/12/1/builder"),
            delegated(protocol::CallerKind::Agent, "cairn://p/cairn/12/1/builder"),
            authority::ServiceRunRole::Standing,
        ] {
            let shut = register(role.clone()).await;
            assert!(
                change(
                    &orch,
                    &shut,
                    "cairn://p/cairn/prep/db/attempts",
                    ChangeMode::Append,
                    json!({"passed": 0}),
                )
                .await
                .is_err(),
                "{role:?}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Fence;

    /// The fence a service run gets is fixed, and a job with no execution
    /// snapshot must not fall through to "no fence configured".
    #[tokio::test]
    async fn a_service_run_is_always_fenced_deny() {
        let (orch, _config) = crate::fleet::service_placement::tests::attached_orchestrator().await;
        let db = orch.db.local.clone();
        testing::seed_project(&db).await;
        let job = rows::ensure_service_job(&db, "p", &ServiceOwner::Project, "echo")
            .await
            .unwrap();
        let run = rows::mint_run(&db, "p", &job).await.unwrap();
        assert_eq!(
            crate::mcp::handlers::permission::resolve_fence_policy(&orch, Some(&run)).await,
            Some(Fence::Deny)
        );

        // A service run the supervisor does not hold is refused at the gate,
        // never served as an ordinary agent run; an ordinary run passes.
        let request = |run_id: &str| crate::mcp::types::McpCallbackRequest {
            tool: "read_batch".into(),
            payload: serde_json::json!({"paths": ["cairn://help"]}),
            run_id: Some(run_id.to_string()),
            ..Default::default()
        };
        let refusal = authority::refuse(&orch, &request(&run))
            .await
            .expect("an unheld service run is refused");
        assert!(refusal.contains("no longer live"), "{refusal}");
        db.execute(
            "INSERT INTO jobs (id, project_id, agent_config_id, status, created_at, updated_at)
             VALUES ('agent-job', 'p', 'build', 'running', 1, 1);
             ",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
             VALUES ('agent-run', 'p', 'agent-job', 'live', 1, 1)",
            (),
        )
        .await
        .unwrap();
        assert!(authority::refuse(&orch, &request("agent-run"))
            .await
            .is_none());
        // Remembered: the second ask does not need the database.
        assert!(authority::refuse(&orch, &request("agent-run"))
            .await
            .is_none());

        // And the run authenticates as the service's own Agent principal.
        let agent = crate::mcp::handlers::run_context::authenticated_agent_for_run(&orch.db, &run)
            .await
            .unwrap();
        assert_eq!(agent.home, "cairn://p/cairn/services/echo");
    }
}
