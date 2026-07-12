//! Best-effort synced visibility for enrolled executor advertisements.

use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_common::executor_protocol::ExecutorAdvertisement;
use cairn_db::turso::params;

const REGISTRY_TTL_SECONDS: i64 = 90; // Three missed 30-second heartbeats marks a row stale.

impl Orchestrator {
    pub async fn upsert_executor_registry_all_teams(
        &self,
        advertisement: &ExecutorAdvertisement,
        generation: u64,
    ) {
        for team_id in self.db.open_team_ids().await {
            if let Some(team_db) = self.db.team_db(&team_id).await {
                upsert_executor(&team_db, advertisement, generation, "online").await;
            }
        }
    }

    pub async fn mark_executor_offline_all_teams(
        &self,
        advertisement: &ExecutorAdvertisement,
        generation: u64,
    ) {
        for team_id in self.db.open_team_ids().await {
            if let Some(team_db) = self.db.team_db(&team_id).await {
                upsert_executor(&team_db, advertisement, generation, "offline").await;
            }
        }
    }
}

pub async fn upsert_executor(
    db: &LocalDb,
    advertisement: &ExecutorAdvertisement,
    generation: u64,
    status: &str,
) {
    let now = chrono::Utc::now().timestamp();
    let expires_at = if status == "online" {
        now + REGISTRY_TTL_SECONDS
    } else {
        now
    };
    let identity = advertisement.identity.clone();
    let capabilities = advertisement.capabilities.clone();
    let toolchains =
        serde_json::to_string(&capabilities.toolchains).unwrap_or_else(|_| "[]".into());
    let projects =
        serde_json::to_string(&capabilities.projects_served).unwrap_or_else(|_| "[]".into());
    let warm = serde_json::to_string(&advertisement.warm_roots).unwrap_or_else(|_| "[]".into());
    let status = status.to_string();
    let current_load = advertisement.current_load as i64;
    let result = db.write(|conn| {
        let identity = identity.clone();
        let capabilities = capabilities.clone();
        let toolchains = toolchains.clone();
        let projects = projects.clone();
        let warm = warm.clone();
        let status = status.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO executor_registry (device_id, executor_id, display_name, os, arch, logical_cores, toolchains, projects_served, slot_capacity, current_load, warm_commits, connection_generation, status, last_seen, expires_at, updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?14) ON CONFLICT(device_id,executor_id) DO UPDATE SET display_name=excluded.display_name, os=excluded.os, arch=excluded.arch, logical_cores=excluded.logical_cores, toolchains=excluded.toolchains, projects_served=excluded.projects_served, slot_capacity=excluded.slot_capacity, current_load=excluded.current_load, warm_commits=excluded.warm_commits, connection_generation=excluded.connection_generation, status=excluded.status, last_seen=excluded.last_seen, expires_at=excluded.expires_at, updated_at=excluded.updated_at",
                params![identity.device_id, identity.executor_id, identity.display_name, capabilities.os, capabilities.arch, capabilities.logical_cores as i64, toolchains, projects, capabilities.slot_capacity as i64, current_load, warm, generation as i64, status, now, expires_at]
            ).await?;
            Ok(())
        })
    }).await;
    if let Err(error) = result {
        log::warn!("executor_registry upsert failed: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, RowExt, TEAM_MIGRATIONS};
    use cairn_common::executor_protocol::{ExecutorCapabilities, ExecutorIdentity};

    #[tokio::test]
    async fn advertisement_upsert_and_disconnect_are_non_secret() {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("team.db")).await.unwrap();
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let advertisement = ExecutorAdvertisement {
            identity: ExecutorIdentity {
                device_id: "d".into(),
                executor_id: "e".into(),
                display_name: "E".into(),
            },
            capabilities: ExecutorCapabilities {
                os: "linux".into(),
                arch: "arm64".into(),
                logical_cores: 4,
                toolchains: vec!["rust".into()],
                projects_served: vec!["p".into()],
                slot_capacity: 2,
                disk_budget_bytes: None,
                memory_budget_bytes: None,
            },
            current_load: 1,
            warm_roots: vec![cairn_common::executor_protocol::VerifiedWarmRoot {
                repository: cairn_common::executor_protocol::RepositoryIdentity {
                    project_id: "p".into(),
                    repository_id: "repo".into(),
                    object_format: cairn_common::executor_protocol::GitObjectFormat::Sha1,
                },
                commit: "abc".into(),
            }],
            observed_at_unix_ms: 1,
        };
        upsert_executor(&db, &advertisement, 3, "online").await;
        upsert_executor(&db, &advertisement, 3, "offline").await;
        let (status, count): (String, i64) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT status, COUNT(*) FROM executor_registry WHERE executor_id='e'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.i64(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(status, "offline");
        assert_eq!(count, 1);
        let columns: Vec<String> = db
            .query_all("PRAGMA table_info(executor_registry)", (), |row| {
                row.text(1)
            })
            .await
            .unwrap();
        assert!(!columns
            .iter()
            .any(|column| column.contains("credential") || column.contains("token")));
    }
}
