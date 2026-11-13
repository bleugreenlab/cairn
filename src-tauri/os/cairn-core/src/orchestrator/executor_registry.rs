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
        self.write_registry_all_teams(advertisement, generation, "online")
            .await;
    }

    pub async fn mark_executor_offline_all_teams(
        &self,
        advertisement: &ExecutorAdvertisement,
        generation: u64,
    ) {
        self.write_registry_all_teams(advertisement, generation, "offline")
            .await;
    }

    async fn write_registry_all_teams(
        &self,
        advertisement: &ExecutorAdvertisement,
        generation: u64,
        status: &str,
    ) {
        let executor_id = &advertisement.identity.executor_id;
        let Some(_permit) = self
            .fleet
            .registry_write_permit(executor_id, generation)
            .await
        else {
            log::info!(
                "executor registry write skipped executor_id={executor_id} generation={generation} status={status}: a newer generation holds the row"
            );
            return;
        };
        for team_id in self.db.open_team_ids().await {
            if let Some(team_db) = self.db.team_db(&team_id).await {
                upsert_executor(&team_db, advertisement, generation, status).await;
            }
        }
    }
}

async fn upsert_executor(
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
    let status = status.to_string();
    let current_load = advertisement.current_load as i64;
    let result = db.write(|conn| {
        let identity = identity.clone();
        let capabilities = capabilities.clone();
        let toolchains = toolchains.clone();
        let projects = projects.clone();
        let status = status.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO executor_registry (device_id, executor_id, display_name, os, arch, logical_cores, toolchains, projects_served, current_load, connection_generation, status, last_seen, expires_at, updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13) ON CONFLICT(device_id,executor_id) DO UPDATE SET display_name=excluded.display_name, os=excluded.os, arch=excluded.arch, logical_cores=excluded.logical_cores, toolchains=excluded.toolchains, projects_served=excluded.projects_served, current_load=excluded.current_load, connection_generation=excluded.connection_generation, status=excluded.status, last_seen=excluded.last_seen, expires_at=excluded.expires_at, updated_at=excluded.updated_at",
                params![identity.device_id, identity.executor_id, identity.display_name, capabilities.os, capabilities.arch, capabilities.logical_cores as i64, toolchains, projects, current_load, generation as i64, status, now, expires_at]
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

    /// A superseded generation's registry writes never land after its
    /// replacement's: one already under way when the replacement attaches
    /// finishes first, and every later one, online or offline, is refused.
    #[tokio::test]
    async fn a_superseded_generations_registry_writes_never_land_after_the_live_ones() {
        let fleet = crate::fleet::Fleet::default();
        let (old_sender, _old_receiver) = tokio::sync::mpsc::unbounded_channel();
        let old = fleet.attach_executor(old_sender);
        let executor_id = fleet.executor_health(0)[0].identity.executor_id.clone();
        let in_flight = fleet
            .registry_write_permit(&executor_id, old)
            .await
            .expect("the only generation may write");

        let (live_sender, _live_receiver) = tokio::sync::mpsc::unbounded_channel();
        let live = fleet.attach_executor(live_sender);
        let waiting = {
            let fleet = fleet.clone();
            let executor_id = executor_id.clone();
            tokio::spawn(async move {
                fleet
                    .registry_write_permit(&executor_id, live)
                    .await
                    .is_some()
            })
        };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            !waiting.is_finished(),
            "the replacement's write waits for the one under way"
        );
        drop(in_flight);
        assert!(waiting.await.unwrap());

        assert!(fleet
            .registry_write_permit(&executor_id, old)
            .await
            .is_none());
        assert!(fleet
            .registry_write_permit(&executor_id, live)
            .await
            .is_some());
    }

    #[tokio::test]
    async fn advertisement_upsert_and_disconnect_are_non_secret() {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("team.db")).await.unwrap();
        // A row an older runner wrote, with every warm root in it, before the
        // migration that empties them.
        let (earlier, clearing): (Vec<_>, Vec<_>) = TEAM_MIGRATIONS
            .iter()
            .copied()
            .partition(|migration| migration.name() != "clear_executor_registry_warm_commits");
        assert_eq!(clearing.len(), 1);
        MigrationRunner::new(earlier).run(&db).await.unwrap();
        db.execute_script(
            "INSERT INTO executor_registry (device_id, executor_id, display_name, os, arch, logical_cores, current_load, warm_commits, connection_generation, status, last_seen, expires_at, updated_at) VALUES ('d', 'e', 'E', 'linux', 'arm64', 4, 1, '[{\"commit\":\"abc\"}]', 2, 'online', 1, 1, 1);",
        )
        .await
        .unwrap();
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
                concurrency_capacity: None,
                toolchains: vec!["rust".into()],
                projects_served: vec!["p".into()],
                disk_budget_bytes: None,
                memory_budget_bytes: None,
                sandbox: None,
                toolchain_detection: None,
                build_prerequisites: None,
                containment: None,
                page_call_programs: None,
                github_credential: None,
                publish_isolation: None,
                machine_services: Vec::new(),
                credential_floor_service_residents: false,
                service_bounds: None,
            },
            current_load: 1,
            observed_at_unix_ms: 1,
            liveness_observed_at_unix_ms: None,
            facts_stale_refresh_blocked: None,
            last_refresh: None,
            node_scratches: Vec::new(),
        };
        upsert_executor(&db, &advertisement, 3, "online").await;
        upsert_executor(&db, &advertisement, 3, "offline").await;
        let (status, count, warm): (String, i64, String) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT status, COUNT(*), MAX(warm_commits) FROM executor_registry WHERE executor_id='e'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.i64(1)?, row.text(2)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(status, "offline");
        assert_eq!(count, 1);
        assert_eq!(warm, "[]", "the registry row carries no warm roots");
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
