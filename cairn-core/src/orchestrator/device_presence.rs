//! Device presence upsert (CAIRN-2629).
//!
//! Each runner advertises a synced per-team `device_presence` row: its stable
//! machine `device_id`, a human name, a fresh `last_seen`, and the project keys
//! it has a LOCAL clone for. The project keys come from the PRIVATE
//! `project_routes` overlay (`local_repo_path IS NOT NULL`) — never the synced
//! `projects.repo_path`, which is the CREATOR's path and means nothing on a
//! teammate's machine. Peers read these rows to build the runner picker, to
//! validate that a chosen device can actually run a project, and to render
//! "waiting for <device>" / "offline" from a stale `last_seen`.
//!
//! Everything here is best-effort: a missing table or a failed write is logged
//! and skipped, never fatal — a stale or absent presence row must not break the
//! picker or the ownership guard.

use std::time::Duration;

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Presence refresh cadence. `last_seen` staleness beyond a few multiples of this
/// is what the UI reads as "offline".
const PRESENCE_INTERVAL: Duration = Duration::from_secs(30);

impl Orchestrator {
    /// Spawn the periodic device-presence upsert loop (one row per open team).
    pub fn spawn_device_presence(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            let device_id = orch.anon_device_manager.device_id();
            let device_name = crate::account::anon_device::machine_device_name();
            let mut interval = tokio::time::interval(PRESENCE_INTERVAL);
            loop {
                interval.tick().await;
                orch.upsert_device_presence_all_teams(&device_id, &device_name)
                    .await;
            }
        });
    }

    /// Upsert this machine's presence row into every currently-open team replica.
    pub async fn upsert_device_presence_all_teams(&self, device_id: &str, device_name: &str) {
        for team_id in self.db.open_team_ids().await {
            let Some(team_db) = self.db.team_db(&team_id).await else {
                continue;
            };
            let keys = local_cloned_project_keys(&self.db.local, &team_id).await;
            upsert_presence(&team_db, device_id, device_name, &keys).await;
        }
    }
}

/// Project keys THIS machine has a local clone for, in one team — read from the
/// private routing overlay (`project_routes.local_repo_path`), NOT the synced
/// project rows (which carry the creator's path).
pub async fn local_cloned_project_keys(private_db: &LocalDb, team_id: &str) -> Vec<String> {
    let team_id = team_id.to_string();
    private_db
        .query_all(
            "SELECT project_key FROM project_routes \
             WHERE team_id = ?1 AND local_repo_path IS NOT NULL",
            (team_id,),
            |row| row.text(0),
        )
        .await
        .unwrap_or_default()
}

/// Upsert one machine's presence row into a team replica. Best-effort: a missing
/// table or write error is logged and swallowed. Timestamps are SECONDS.
pub async fn upsert_presence(
    team_db: &LocalDb,
    device_id: &str,
    device_name: &str,
    project_keys: &[String],
) {
    let json = serde_json::to_string(project_keys).unwrap_or_else(|_| "[]".to_string());
    let now = chrono::Utc::now().timestamp();
    let device_id = device_id.to_string();
    let device_name = device_name.to_string();
    let result = team_db
        .write(|conn| {
            let device_id = device_id.clone();
            let device_name = device_name.clone();
            let json = json.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO device_presence \
                        (device_id, device_name, last_seen, project_keys, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?3) \
                     ON CONFLICT(device_id) DO UPDATE SET \
                        device_name = excluded.device_name, \
                        last_seen = excluded.last_seen, \
                        project_keys = excluded.project_keys, \
                        updated_at = excluded.updated_at",
                    (device_id.as_str(), device_name.as_str(), now, json.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await;
    if let Err(error) = result {
        log::warn!("device_presence upsert failed: {error}");
    }
}

/// A device-presence row as read for the runner picker / owner UI.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DevicePresence {
    pub device_id: String,
    pub device_name: String,
    pub last_seen: i64,
    /// Project keys this device has a local clone for.
    pub project_keys: Vec<String>,
    pub updated_at: i64,
}

/// Read all device-presence rows from one team replica (newest heartbeat first).
/// Tolerant of a replica whose table is absent: returns an empty list rather than
/// erroring, so the picker/guard never break on a legacy replica.
pub async fn list_device_presence(team_db: &LocalDb) -> Vec<DevicePresence> {
    let rows: Result<Vec<DevicePresence>, _> = team_db
        .query_all(
            "SELECT device_id, device_name, last_seen, project_keys, updated_at \
             FROM device_presence ORDER BY last_seen DESC",
            (),
            |row| {
                let project_keys: Vec<String> =
                    serde_json::from_str(&row.text(3)?).unwrap_or_default();
                Ok(DevicePresence {
                    device_id: row.text(0)?,
                    device_name: row.text(1)?,
                    last_seen: row.i64(2)?,
                    project_keys,
                    updated_at: row.i64(4)?,
                })
            },
        )
        .await;
    rows.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TEAM_MIGRATIONS, TURSO_MIGRATIONS};

    async fn team_db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("team.db")).await.unwrap();
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn private_db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("private.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn upsert_then_read_roundtrips() {
        let db = team_db().await;
        upsert_presence(&db, "devA", "mac (macos)", &["PROJ".into()]).await;
        let rows = list_device_presence(&db).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].device_id, "devA");
        assert_eq!(rows[0].device_name, "mac (macos)");
        assert_eq!(rows[0].project_keys, vec!["PROJ".to_string()]);
        assert!(rows[0].last_seen > 0);
    }

    #[tokio::test]
    async fn upsert_is_idempotent_on_device_id() {
        let db = team_db().await;
        upsert_presence(&db, "devA", "old-name", &[]).await;
        upsert_presence(&db, "devA", "new-name", &["P".into()]).await;
        let rows = list_device_presence(&db).await;
        assert_eq!(rows.len(), 1, "one row per device_id");
        assert_eq!(rows[0].device_name, "new-name");
        assert_eq!(rows[0].project_keys, vec!["P".to_string()]);
    }

    #[tokio::test]
    async fn project_keys_come_from_cloned_routes_only() {
        let db = private_db().await;
        // project_routes.team_id FKs the private routing `teams` registry.
        db.execute(
            "INSERT INTO teams (id, name, sync_url, replica_path, created_at) \
             VALUES ('team1', 'Team', 'http://sync', '/tmp/t.db', 0)",
            (),
        )
        .await
        .unwrap();
        // Two routes for one team: one cloned locally, one not.
        db.execute(
            "INSERT INTO project_routes (project_key, team_id, local_repo_path, created_at) \
             VALUES ('CLONED', 'team1', '/tmp/x', 0)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO project_routes (project_key, team_id, local_repo_path, created_at) \
             VALUES ('UNCLONED', 'team1', NULL, 0)",
            (),
        )
        .await
        .unwrap();
        let keys = local_cloned_project_keys(&db, "team1").await;
        assert_eq!(keys, vec!["CLONED".to_string()]);
    }

    #[tokio::test]
    async fn list_tolerates_absent_table() {
        // A private DB has no device_presence table; reading must not panic.
        let db = private_db().await;
        assert!(list_device_presence(&db).await.is_empty());
    }
}
