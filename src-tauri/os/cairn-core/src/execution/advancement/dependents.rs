use super::*;

pub async fn release_dependent_executions(
    orch: &Orchestrator,
    resolved_issue_id: &str,
) -> Result<(), String> {
    cairn_workflow::advancement::release_dependent_executions(
        &crate::workflow_host::runtime(orch),
        resolved_issue_id,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::release_dependent_executions;
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{
        LocalDb, MigrationRunner, RowExt, SearchIndex, TEAM_MIGRATIONS, TURSO_MIGRATIONS,
    };
    use cairn_db::turso::params;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestOrch {
        _temp: TempDir,
        orch: Orchestrator,
    }

    async fn test_orch() -> TestOrch {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let local = Arc::new(
            LocalDb::open(temp.path().join("dependents-local.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search_index =
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let db = Arc::new(DbState::new(local, search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(db, services, config_dir).build();
        TestOrch { _temp: temp, orch }
    }

    async fn migrated_team_db(temp: &TempDir) -> Arc<LocalDb> {
        let db = Arc::new(
            LocalDb::open(temp.path().join("dependents-team.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    /// Seed a team replica with a MERGED upstream issue and a running downstream
    /// execution that depends on it. The dependency is met (upstream merged), so
    /// releasing the upstream issue enqueues the downstream execution's
    /// `advance_dag`. Both issues, the dependency edge, and the execution live
    /// WHOLLY in the replica — the read side reads them from the routed db.
    async fn seed_team_dependency_chain(
        db: &LocalDb,
        resolved_issue_id: &str,
        dependent_issue_id: &str,
        dependent_execution_id: &str,
    ) {
        let project_id = format!(
            "{}~00000000-0000-4000-8000-200000000001",
            resolved_issue_id.split('~').next().unwrap()
        );
        let team_id = resolved_issue_id.split('~').next().unwrap();
        db.execute(
            "INSERT INTO teams(id,name,created_at,updated_at) VALUES(?1,'Team',1,1)",
            params![team_id],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO projects (id, team_id, name, key, repo_path, created_at, updated_at) VALUES (?1, ?2, 'Team Project', 'tp', '/tmp/team-project', 1, 1)",
            params![project_id.as_str(), team_id],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, merged_at, created_at, updated_at) VALUES (?1, ?2, 10, 'Upstream', 'merged', 2, 1, 2)",
            params![resolved_issue_id, project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES (?1, ?2, 11, 'Downstream', 'active', 1, 1)",
            params![dependent_issue_id, project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issue_dependencies (issue_id, depends_on_uri, created_at) VALUES (?1, 'cairn://p/tp/10', 1)",
            params![dependent_issue_id],
        )
        .await
        .unwrap();
        let snapshot = serde_json::json!({
            "recipe": {
                "id": "recipe",
                "name": "Recipe",
                "description": null,
                "trigger": "manual",
                "nodes": [
                    {
                        "id": "agent",
                        "nodeType": "agent",
                        "name": "builder",
                        "position": { "x": 0.0, "y": 0.0 },
                        "agentConfig": { "agentConfigId": null }
                    }
                ],
                "edges": []
            },
            "agents": {},
            "skills": {},
            "triggerContext": {
                "issueId": dependent_issue_id,
                "projectId": project_id,
                "triggerType": "manual"
            },
            "delegatedPackets": [],
            "createdAt": 1
        })
        .to_string();
        db.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES (?1, 'recipe', ?2, ?3, 'running', 1, 1, ?4)",
            params![
                dependent_execution_id,
                dependent_issue_id,
                project_id.as_str(),
                snapshot.as_str()
            ],
        )
        .await
        .unwrap();
    }

    async fn advance_dag_count(db: &LocalDb, execution_id: &str) -> i64 {
        db.query_one(
            "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag' AND dedupe_key = ?1",
            params![execution_id],
            |row| row.i64(0),
        )
        .await
        .unwrap()
    }

    /// Team state stays in the replica; advancement is replayed by the private host outbox.
    #[tokio::test]
    async fn dependent_team_execution_advance_dag_uses_private_outbox() {
        let test = test_orch().await;
        let team_temp = tempfile::tempdir().unwrap();
        let team_id = "teamdep";
        let resolved_issue_id = "teamdep~00000000-0000-4000-8000-200000000010";
        let dependent_issue_id = "teamdep~00000000-0000-4000-8000-200000000011";
        let dependent_execution_id = "teamdep~00000000-0000-4000-8000-200000000012";
        let team_db = migrated_team_db(&team_temp).await;
        seed_team_dependency_chain(
            &team_db,
            resolved_issue_id,
            dependent_issue_id,
            dependent_execution_id,
        )
        .await;
        test.orch
            .db
            .insert_team_db_for_test(team_id, team_db.clone())
            .await;

        release_dependent_executions(&test.orch, resolved_issue_id)
            .await
            .unwrap();

        assert_eq!(
            team_db
                .query_one(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'effect_outbox'",
                    (),
                    |row| row.i64(0)
                )
                .await
                .unwrap(),
            0,
            "the real team schema has no private outbox"
        );
        assert_eq!(
            advance_dag_count(&test.orch.db.local, dependent_execution_id).await,
            1,
            "advancement must be durably queued where startup recovery drains it"
        );
    }

    /// Fail-closed: a team resolved issue whose replica is not open errors rather
    /// than silently falling back to the private DB, and writes nothing there.
    #[tokio::test]
    async fn release_fails_closed_when_team_replica_not_open() {
        let test = test_orch().await;
        let resolved_issue_id = "teamdep~00000000-0000-4000-8000-200000000010";

        // The team replica is never inserted, so routing the resolved issue fails
        // closed rather than draining anything into the private DB.
        assert!(release_dependent_executions(&test.orch, resolved_issue_id)
            .await
            .is_err());
        assert_eq!(
            test.orch
                .db
                .local
                .query_one(
                    "SELECT COUNT(*) FROM effect_outbox WHERE kind = 'advance_dag'",
                    (),
                    |row| row.i64(0),
                )
                .await
                .unwrap(),
            0,
            "a fail-closed release must not write any advance_dag row to the private DB"
        );
    }
}
