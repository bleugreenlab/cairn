//! A node job's model: chosen once, at the job's own launch, and stored.
//!
//! Selection waits until the node launches because supply moves hourly and a
//! constrained node (a reviewer) must read the model its upstream node
//! actually stored. The stored record is the job's answer for the rest of its
//! life: a resumed, continued or retried job reads it back and is never
//! re-selected.

use super::*;
use crate::execution::model_selection::{
    effective_allotment, ResolvedConstraints, SelectionRecord, SelectionSubject,
};
use crate::models::ModelSelection;
use cairn_config::allotment::NodeConstraints;
use cairn_models::types::catalog::VerbKind;

/// Choose and store this job's model, unless it already has one.
///
/// Runs before the session is prepared (a node's cold start, and every
/// resume), so the session row is opened on the chosen provider. A job that
/// carries a model already (a launch pin written at job creation, an
/// operator's edit) or a stored selection keeps it. A recipe node selects
/// within its node's allotment under its constraints; a job outside any
/// recipe node (a thread's session job) selects within its agent's. A
/// refusal fails the launch with the refusal's text.
pub(super) fn select_job_model(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    project_path: Option<&std::path::Path>,
) -> Result<(), String> {
    let Some(agent_id) = job.agent_config_id.as_deref() else {
        return Ok(());
    };
    if orch.job_selection(&job.id)?.is_some() {
        return Ok(());
    }
    let snapshot = match job.execution_id.as_deref() {
        Some(execution_id) => load_snapshot(owning_db, execution_id)?,
        None => None,
    };
    let agent = snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.agents.get(agent_id));
    let subject = SelectionSubject {
        issue_id: job.issue_id.clone(),
        execution_id: job.execution_id.clone(),
        node_id: job.recipe_node_id.clone(),
        job_id: Some(job.id.clone()),
    };

    // A pin is the job's choice: it stores a record like any other, so a
    // constraint naming this job reads what it actually runs on.
    if let Some(pin) = pinned(
        job.model.as_deref(),
        agent.and_then(|agent| agent.selection.as_ref()),
    ) {
        let record = orch.pinned_selection(Some(&job.project_id), &pin, subject)?;
        log::info!("[select] job {}: {}", job.id, record.provenance);
        return orch.store_job_selection(&job.id, &record);
    }

    match (snapshot.as_ref(), job.recipe_node_id.as_deref()) {
        (Some(snapshot), Some(node_id)) => {
            select_node_model(orch, owning_db, job, snapshot, agent, node_id, subject)
        }
        (_, None) => {
            select_host_model(orch, owning_db, job, agent_id, agent, project_path, subject)
        }
        (None, Some(_)) => match job.execution_id.as_deref() {
            Some(execution_id) => Err(format!("Execution {execution_id} has no snapshot")),
            None => Ok(()),
        },
    }
}

/// The model a job is pinned to: its launch fact, which wins (CAIRN-3798),
/// else its snapshot agent's selection (the composer's or the snapshot
/// editor's pin). The agent's selection supplies the provider when it names
/// the same model.
fn pinned(job_model: Option<&str>, agent: Option<&ModelSelection>) -> Option<ModelSelection> {
    match (job_model, agent) {
        (Some(model), Some(selection)) if selection.model.as_str() == model => {
            Some(selection.clone())
        }
        (Some(model), _) => Some(ModelSelection::new(
            crate::backends::resolved_backend_for_model(model),
            Model::new(model),
        )),
        (None, selection) => selection.cloned(),
    }
}

fn load_snapshot(
    owning_db: &Arc<LocalDb>,
    execution_id: &str,
) -> Result<Option<ExecutionSnapshot>, String> {
    run_db({
        let db = owning_db.clone();
        let execution_id = execution_id.to_string();
        async move {
            db.read(move |conn| {
                let execution_id = execution_id.clone();
                Box::pin(async move { load_execution_snapshot_conn(conn, &execution_id).await })
            })
            .await
            .map_err(|e| db_error("Failed to load execution snapshot", e))
        }
    })
}

/// A job outside any recipe node selects once, at its first launch, within
/// its agent's allotment. One that has already run on a provider stays
/// there: choosing now would move a live conversation to another provider.
fn select_host_model(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    agent_id: &str,
    agent: Option<&AgentSnapshot>,
    project_path: Option<&std::path::Path>,
    subject: SelectionSubject,
) -> Result<(), String> {
    if has_run_on_a_provider(owning_db, &job.id)? {
        return Ok(());
    }
    let models = match agent {
        Some(agent) => agent.models.clone(),
        None => {
            crate::config::agents::get_agent(&orch.config_dir, agent_id, project_path)
                .map_err(|e| format!("Failed to load agent config '{agent_id}': {e}"))?
                .ok_or_else(|| format!("Agent config '{agent_id}' not found"))?
                .models
        }
    };
    let record = orch
        .select_for_agent(&job.project_id, None, &models, subject)
        .map_err(|refusal| format!("{agent_id} cannot launch: {refusal}"))?;
    log::info!("[select] job {}: {}", job.id, record.provenance);
    store_node_model(owning_db, &job.id, &record)
}

/// The provider of the session `job_id` is prepared to fork, when its current
/// session forks another job's (a `session: fork` task) and has not started.
/// A rotation successor also names a parent session, but continues its own
/// job's conversation rather than forking one.
fn forked_provider(owning_db: &Arc<LocalDb>, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    run_db({
        let db = owning_db.clone();
        async move {
            db.read(move |conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT s.backend FROM jobs j
                             JOIN sessions s ON s.id = j.current_session_id
                             JOIN sessions parent ON parent.id = s.parent_session_id
                             WHERE j.id = ?1
                               AND s.backend_id IS NULL
                               AND parent.job_id IS NOT j.id",
                            params![job_id.as_str()],
                        )
                        .await?;
                    match rows.next().await? {
                        Some(row) => Ok(Some(row.get::<String>(0)?)),
                        None => Ok(None),
                    }
                })
            })
            .await
            .map_err(|e| db_error("Failed to read the job's session", e))
        }
    })
}

/// Whether any session of `job_id` has been confirmed by a provider.
fn has_run_on_a_provider(owning_db: &Arc<LocalDb>, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    run_db({
        let db = owning_db.clone();
        async move {
            db.read(move |conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT 1 FROM sessions
                             WHERE job_id = ?1 AND backend_id IS NOT NULL LIMIT 1",
                            params![job_id.as_str()],
                        )
                        .await?;
                    Ok(rows.next().await?.is_some())
                })
            })
            .await
            .map_err(|e| db_error("Failed to read the job's sessions", e))
        }
    })
}

/// A recipe node selects within its node's allotment, else its agent's, under
/// the node's constraints.
fn select_node_model(
    orch: &Orchestrator,
    owning_db: &Arc<LocalDb>,
    job: &DbJob,
    snapshot: &ExecutionSnapshot,
    agent: Option<&AgentSnapshot>,
    node_id: &str,
    subject: SelectionSubject,
) -> Result<(), String> {
    let execution_id = job.execution_id.as_deref().unwrap_or_default();
    let node = snapshot.recipe.nodes.iter().find(|node| node.id == node_id);
    let node_config = node.and_then(|node| node.agent_config.as_ref());
    let label = node.map_or(node_id, |node| node.name.as_str());
    let defaults = orch.effective_models(Some(&job.project_id))?.default_models;
    let allotment = effective_allotment(
        node_config.and_then(|config| config.models.as_deref()),
        agent.map_or(&[][..], |agent| agent.models.as_slice()),
        &defaults,
    );
    let mut constraints = match node_config {
        Some(config) => resolve_constraints(orch, execution_id, snapshot, &config.constraints)?,
        None => ResolvedConstraints::default(),
    };
    constraints.fork_provider = forked_provider(owning_db, &job.id)?;
    let record = orch
        .select_models(
            Some(&job.project_id),
            &allotment,
            VerbKind::Session,
            &constraints,
            subject,
        )
        .map_err(|refusal| format!("{label} cannot launch: {refusal}"))?;
    log::info!("[select] job {}: {}", job.id, record.provenance);
    store_node_model(owning_db, &job.id, &record)
}

/// The stored selections a node's constraints name, refusing a reference
/// whose node has not launched: a constraint is a statement about a choice
/// already made, and every node (a pinned one too) stores its choice when it
/// launches.
fn resolve_constraints(
    orch: &Orchestrator,
    execution_id: &str,
    snapshot: &ExecutionSnapshot,
    constraints: &NodeConstraints,
) -> Result<ResolvedConstraints, String> {
    let mut resolved = ResolvedConstraints::default();
    for (name, slot, reference) in [
        (
            "sameBandAs",
            &mut resolved.same_band_as,
            &constraints.same_band_as,
        ),
        (
            "differentMakerFrom",
            &mut resolved.different_maker_from,
            &constraints.different_maker_from,
        ),
    ] {
        let Some(reference) = reference else {
            continue;
        };
        let upstream = snapshot
            .recipe
            .nodes
            .iter()
            .find(|node| node.id == *reference)
            .map_or(reference.as_str(), |node| node.name.as_str())
            .to_string();
        let record = orch.node_selection(execution_id, reference)?.ok_or_else(|| {
            format!(
                "{name} names '{upstream}', which has not launched yet; a constraint reads the choice its node stores when it launches"
            )
        })?;
        // The stored provenance names the upstream node as the recipe shows
        // it, not by its id.
        *slot = Some((upstream, record));
    }
    Ok(resolved)
}

/// Write the chosen model and its record onto the job, and move a session
/// that has never started onto the chosen provider in the same transaction,
/// so the session row never names a provider its process will not run on.
fn store_node_model(
    owning_db: &Arc<LocalDb>,
    job_id: &str,
    record: &SelectionRecord,
) -> Result<(), String> {
    let json = serde_json::to_string(record)
        .map_err(|error| format!("a selection does not serialize: {error}"))?;
    let model = record.chosen.model.as_str().to_string();
    let backend = record.chosen.backend.clone();
    let job_id = job_id.to_string();
    run_db({
        let db = owning_db.clone();
        async move {
            db.write(move |conn| {
                let (job_id, json, model, backend) =
                    (job_id.clone(), json.clone(), model.clone(), backend.clone());
                Box::pin(async move {
                    conn.execute(
                        "UPDATE jobs SET model = ?1, selection = ?2, updated_at = ?3 WHERE id = ?4",
                        params![
                            model.as_str(),
                            json.as_str(),
                            chrono::Utc::now().timestamp(),
                            job_id.as_str()
                        ],
                    )
                    .await?;
                    conn.execute(
                        "UPDATE sessions SET backend = ?1
                         WHERE id = (SELECT current_session_id FROM jobs WHERE id = ?2)
                           AND status = 'open' AND backend_id IS NULL",
                        params![backend.as_str(), job_id.as_str()],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| db_error("Failed to store the job's selection", e))
        }
    })
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::models::UpdateSettings;
    use crate::orchestrator::models::tests::bare_orchestrator;
    use cairn_config::allotment::{
        model_ref, parse_model_ref, Level, ModelsConfig, Pool, PoolKind, Rung,
    };
    use cairn_models::types::catalog::{claude_models, DiscoveredModel, ProviderModelCatalog};

    fn band(id: &str, min: f64) -> Pool {
        Pool {
            id: id.into(),
            description: format!("{id} work"),
            kind: PoolKind::Band { min },
        }
    }

    fn rung(model: &str, level: f64) -> Rung {
        Rung {
            model: parse_model_ref(model).unwrap(),
            level: Some(Level::set(level)),
            effort: None,
            effort_level: None,
            options: Default::default(),
        }
    }

    /// sm, md and lg bands over `rungs`.
    fn ladder(rungs: &[(&str, f64)]) -> ModelsConfig {
        ModelsConfig {
            pools: vec![band("sm", 0.0), band("md", 0.34), band("lg", 0.67)],
            ladder: rungs
                .iter()
                .map(|(model, level)| rung(model, *level))
                .collect(),
            ..ModelsConfig::default()
        }
    }

    /// Each band holds one Anthropic and one OpenAI model, OpenAI's higher,
    /// so an unconstrained node picks OpenAI and a reviewer must not.
    pub(in crate::execution::jobs) fn two_makers() -> ModelsConfig {
        ladder(&[
            ("claude/haiku", 0.17),
            ("claude/sonnet", 0.45),
            ("codex/gpt-5.6-terra", 0.5),
            ("claude/opus", 0.83),
            ("codex/gpt-5.6-sol", 0.85),
        ])
    }

    /// A Codex model as discovery lists it, with a three-level effort scale.
    fn chat(id: &str) -> DiscoveredModel {
        let mut model = claude_models().remove(0);
        model.id = id.into();
        model.model = id.into();
        model.display_name = id.into();
        model.supported_reasoning_efforts = ["low", "medium", "high"]
            .into_iter()
            .map(
                |effort| cairn_models::types::catalog::DiscoveredReasoningEffort {
                    reasoning_effort: effort.into(),
                    description: None,
                },
            )
            .collect();
        model
    }

    fn agent_node(
        id: &str,
        agent: &str,
        models: Option<&[&str]>,
        upstream: Option<&str>,
    ) -> serde_json::Value {
        let mut config = serde_json::json!({ "agentConfigId": agent });
        if let Some(models) = models {
            config["models"] = serde_json::json!(models);
        }
        if let Some(upstream) = upstream {
            config["constraints"] =
                serde_json::json!({ "sameBandAs": upstream, "differentMakerFrom": upstream });
        }
        serde_json::json!({
            "id": id, "nodeType": "agent", "name": id,
            "position": { "x": 0.0, "y": 0.0 }, "agentConfig": config
        })
    }

    /// `orch` selects within `models` on Claude and Codex, with no accounts, so
    /// supply orders by ladder level.
    pub(in crate::execution::jobs) fn serve_claude_and_codex(
        orch: &Orchestrator,
        models: ModelsConfig,
    ) {
        orch.update_settings(UpdateSettings {
            enabled_providers: Some(vec!["claude".into(), "codex".into()]),
            models: Some(models),
            ..Default::default()
        })
        .unwrap();
        for (backend, models) in [
            ("claude", claude_models()),
            ("codex", vec![chat("gpt-5.6-sol"), chat("gpt-5.6-terra")]),
        ] {
            orch.seed_model_catalog_for_test(ProviderModelCatalog {
                backend: backend.into(),
                models,
                options: Vec::new(),
                refreshed_at: Some(1),
                error: None,
            });
        }
    }

    /// One recipe node of a test execution: its id, its own allotment, and
    /// the upstream node its constraints name.
    type NodeSpec<'a> = (&'a str, Option<&'a [&'a str]>, Option<&'a str>);

    /// A workspace selecting within `models` on Claude and Codex, and one
    /// execution whose recipe has a job per node.
    async fn execution(
        models: ModelsConfig,
        nodes: &[NodeSpec<'_>],
    ) -> (Orchestrator, Arc<LocalDb>, tempfile::TempDir) {
        let orch = bare_orchestrator().await;
        serve_claude_and_codex(&orch, models);
        let agent = |id: &str| {
            serde_json::json!({
                "id": id, "name": id, "description": "", "prompt": "p",
                "tools": [], "models": ["sm", "md", "lg"]
            })
        };
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "planbuild", "name": "PlanBuild", "description": null, "trigger": "manual",
                "nodes": nodes.iter().map(|(id, models, upstream)| {
                    let agent = if upstream.is_some() { "review" } else { "build" };
                    agent_node(id, agent, *models, *upstream)
                }).collect::<Vec<_>>(),
                "edges": []
            },
            "agents": { "build": agent("build"), "review": agent("review") },
            "skills": {},
            "triggerContext": { "issueId": null, "projectId": "p", "triggerType": "manual" },
            "createdAt": 1
        });
        let repo = tempfile::tempdir().unwrap();
        let db = orch.db.local.clone();
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'default', 'P', 'p', ?1, 1, 1)",
            params![repo.path().to_string_lossy().to_string()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions (id, recipe_id, project_id, status, started_at, snapshot)
             VALUES ('x', 'planbuild', 'p', 'running', 1, ?1)",
            params![snapshot.to_string()],
        )
        .await
        .unwrap();
        for (index, (node, _, upstream)) in nodes.iter().enumerate() {
            let agent = if upstream.is_some() {
                "review"
            } else {
                "build"
            };
            db.execute(
                "INSERT INTO jobs (id, project_id, execution_id, recipe_node_id, agent_config_id,
                                   status, created_at, updated_at)
                 VALUES (?1, 'p', 'x', ?1, ?2, 'pending', ?3, ?3)",
                params![*node, agent, index as i64 + 1],
            )
            .await
            .unwrap();
        }
        (orch, db, repo)
    }

    fn job(db: &Arc<LocalDb>, id: &str) -> DbJob {
        run_db(load_job(db.clone(), id.to_string(), "test job")).unwrap()
    }

    fn launch(orch: &Orchestrator, db: &Arc<LocalDb>, id: &str) -> Result<SelectionRecord, String> {
        select_job_model(orch, db, &job(db, id), None)?;
        Ok(orch
            .job_selection(id)?
            .expect("a launched node stores its selection"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_node_stores_its_selection_at_launch_and_a_resumed_job_keeps_it() {
        let (orch, db, _repo) = execution(two_makers(), &[("builder-1", None, None)]).await;
        let stored = launch(&orch, &db, "builder-1").unwrap();
        assert_eq!(model_ref(&stored.chosen), "codex/gpt-5.6-sol");
        assert_eq!(stored.subject.job_id.as_deref(), Some("builder-1"));
        assert_eq!(job(&db, "builder-1").model.as_deref(), Some("gpt-5.6-sol"));

        // The ladder moves: a node launching now would take opus.
        orch.update_settings(UpdateSettings {
            models: Some(ladder(&[("claude/opus", 0.9), ("codex/gpt-5.6-sol", 0.7)])),
            ..Default::default()
        })
        .unwrap();
        let now = orch
            .preview_selection(Some("p"), Some(&["lg".to_string()]), VerbKind::Session)
            .unwrap();
        assert_eq!(model_ref(&now.chosen), "claude/opus");

        // Resumed: the job keeps its launch fact and its record.
        select_job_model(&orch, &db, &job(&db, "builder-1"), None).unwrap();
        assert_eq!(
            orch.job_selection("builder-1").unwrap().as_ref(),
            Some(&stored)
        );
        // The stored record alone is enough, even with the launch fact gone.
        db.execute("UPDATE jobs SET model = NULL WHERE id = 'builder-1'", ())
            .await
            .unwrap();
        select_job_model(&orch, &db, &job(&db, "builder-1"), None).unwrap();
        assert_eq!(
            orch.job_selection("builder-1").unwrap().as_ref(),
            Some(&stored)
        );
        // Every turn's agent runs on the stored choice and its effort.
        let agent = load_agent_config(&orch, &job(&db, "builder-1"), None)
            .unwrap()
            .unwrap();
        assert_eq!(
            agent.selection.as_ref().map(model_ref).as_deref(),
            Some("codex/gpt-5.6-sol")
        );
        assert_eq!(agent.extras.as_ref(), Some(&stored.extras));
    }

    /// A `session: fork` task selects only where the session it forks lives,
    /// and is refused, naming its allotment and that provider, when its
    /// allotment has nothing there.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_forking_task_selects_on_its_forked_sessions_provider() {
        let codex_only: &[&str] = &["codex/gpt-5.6-sol"];
        let (orch, db, _repo) = execution(
            two_makers(),
            &[
                ("task-fork", None, None),
                ("task-codex", Some(codex_only), None),
            ],
        )
        .await;
        db.execute(
            "INSERT INTO jobs (id, project_id, status, current_session_id, created_at, updated_at)
             VALUES ('parent', 'p', 'running', 'ps', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at, backend_id)
             VALUES ('ps', 'parent', 'claude', 'open', 1, 1, 1, 'cli-1')",
            (),
        )
        .await
        .unwrap();
        for (job_id, session) in [("task-fork", "fs1"), ("task-codex", "fs2")] {
            db.execute(
                "INSERT INTO sessions (id, job_id, backend, status, parent_session_id, sequence, created_at, updated_at)
                 VALUES (?1, ?2, 'claude', 'open', 'ps', 2, 1, 1)",
                params![session, job_id],
            )
            .await
            .unwrap();
            db.execute(
                "UPDATE jobs SET current_session_id = ?1 WHERE id = ?2",
                params![session, job_id],
            )
            .await
            .unwrap();
        }

        // Unrestricted, [sm, md, lg] takes Codex's sol; the fork keeps it on
        // Claude, where the parent's session lives.
        let forked = launch(&orch, &db, "task-fork").unwrap();
        assert_eq!(model_ref(&forked.chosen), "claude/opus");
        assert!(
            forked
                .candidates
                .iter()
                .any(|c| c.model == "codex/gpt-5.6-sol" && c.note.starts_with("not on claude")),
            "{:?}",
            forked.candidates
        );

        let refused = launch(&orch, &db, "task-codex").unwrap_err();
        assert!(
            refused.contains(
                "this launch forks a claude session, and [codex/gpt-5.6-sol] has no claude model"
            ),
            "{refused}"
        );
        assert_eq!(orch.job_selection("task-codex").unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_reviewer_reads_its_builders_stored_choice_and_relaxes_only_the_band() {
        let terra_lg: &[&str] = &["codex/gpt-5.6-terra", "lg"];
        let md_sol: &[&str] = &["md", "codex/gpt-5.6-sol"];
        let openai_only: &[&str] = &["codex/gpt-5.6-sol", "codex/gpt-5.6-terra"];
        let (orch, db, _repo) = execution(
            two_makers(),
            &[
                ("builder-1", Some(&["md"]), None),
                ("builder-2", Some(&["lg"]), None),
                ("review-same", None, Some("builder-1")),
                ("review-up", Some(terra_lg), Some("builder-1")),
                ("review-down", Some(md_sol), Some("builder-2")),
                ("review-none", Some(openai_only), Some("builder-2")),
                ("builder-pinned", None, None),
                ("review-pinned", None, Some("builder-pinned")),
            ],
        )
        .await;

        // Before its builder launches there is no choice to read.
        let early = launch(&orch, &db, "review-same").unwrap_err();
        assert!(
            early.contains("sameBandAs names 'builder-1', which has not launched yet"),
            "{early}"
        );
        assert_eq!(orch.job_selection("review-same").unwrap(), None);

        assert_eq!(
            model_ref(&launch(&orch, &db, "builder-1").unwrap().chosen),
            "codex/gpt-5.6-terra"
        );
        assert_eq!(
            model_ref(&launch(&orch, &db, "builder-2").unwrap().chosen),
            "codex/gpt-5.6-sol"
        );

        // The builder's band, another maker.
        let same = launch(&orch, &db, "review-same").unwrap();
        assert_eq!(model_ref(&same.chosen), "claude/sonnet");
        assert_eq!(same.band.as_deref(), Some("md"));
        assert!(same.relaxations.is_empty(), "{:?}", same.relaxations);

        // md holds only the builder's maker here: up to lg first.
        let up = launch(&orch, &db, "review-up").unwrap();
        assert_eq!(model_ref(&up.chosen), "claude/opus");
        assert!(
            up.relaxations
                .iter()
                .any(|r| r.starts_with("band relaxed md→lg")),
            "{:?}",
            up.relaxations
        );

        // Nothing above lg: down to md.
        let down = launch(&orch, &db, "review-down").unwrap();
        assert_eq!(model_ref(&down.chosen), "claude/sonnet");
        assert!(
            down.relaxations
                .iter()
                .any(|r| r.starts_with("band relaxed lg→md")),
            "{:?}",
            down.relaxations
        );

        // A pinned builder stores its pin as its choice, so its reviewer has
        // one to read: the band of the pin's ladder entry, its maker.
        db.execute(
            "UPDATE jobs SET model = 'opus' WHERE id = 'builder-pinned'",
            (),
        )
        .await
        .unwrap();
        let pin = launch(&orch, &db, "builder-pinned").unwrap();
        assert_eq!(model_ref(&pin.chosen), "claude/opus");
        assert_eq!(pin.provenance, "pinned to claude/opus");
        assert_eq!(pin.band.as_deref(), Some("lg"));
        assert_eq!(pin.maker.as_deref(), Some("anthropic"));
        assert_eq!(job(&db, "builder-pinned").model.as_deref(), Some("opus"));
        let reads_pin = launch(&orch, &db, "review-pinned").unwrap();
        assert_eq!(model_ref(&reads_pin.chosen), "codex/gpt-5.6-sol");
        assert!(
            reads_pin
                .provenance
                .ends_with("maker openai differs from builder-pinned's anthropic"),
            "{}",
            reads_pin.provenance
        );

        // Every band holds only OpenAI: the maker rule gives way rather than
        // refusing the launch, and the stored line says so.
        let open = launch(&orch, &db, "review-none").unwrap();
        assert_eq!(model_ref(&open.chosen), "codex/gpt-5.6-sol");
        assert!(
            open.provenance.ends_with(
                "no model from a maker other than openai in [codex/gpt-5.6-sol, codex/gpt-5.6-terra]; same maker as builder-2"
            ),
            "{}",
            open.provenance
        );
        assert_eq!(
            job(&db, "review-none").model.as_deref(),
            Some("gpt-5.6-sol")
        );
    }
}
