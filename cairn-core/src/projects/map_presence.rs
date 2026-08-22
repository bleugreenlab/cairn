use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::params;
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceRow {
    pub job_id: String,
    pub issue_number: Option<i64>,
    pub role: String,
    pub agent_config_id: Option<String>,
    pub verb: String,
    pub file_paths: Vec<String>,
    pub ts_ms: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TouchRow {
    pub file_path: String,
    pub job_id: String,
    pub action: String,
    pub ts_ms: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MapPresenceView {
    pub presence: Vec<PresenceRow>,
    pub touches: Vec<TouchRow>,
}

pub async fn read(
    db: &LocalDb,
    project_id: &str,
    since_ms: i64,
    until_ms: Option<i64>,
) -> DbResult<MapPresenceView> {
    let until_ms = until_ms.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    let presence = db
        .query_all(
            "SELECT p.job_id, p.issue_number, p.role, p.agent_config_id,
                    p.verb, p.file_paths, p.ts_ms
               FROM job_presence p JOIN jobs j ON j.id = p.job_id
              WHERE p.project_id = ?1 AND p.ts_ms >= ?2 AND p.ts_ms <= ?3
              ORDER BY p.ts_ms DESC",
            params![project_id.to_string(), since_ms, until_ms],
            |row| {
                let encoded = row.text(5)?;
                Ok(PresenceRow {
                    job_id: row.text(0)?,
                    issue_number: row.opt_i64(1)?,
                    role: row.opt_text(2)?.unwrap_or_default(),
                    agent_config_id: row.opt_text(3)?,
                    verb: row.text(4)?,
                    file_paths: serde_json::from_str(&encoded).unwrap_or_default(),
                    ts_ms: row.i64(6)?,
                })
            },
        )
        .await?;

    let touches = db
        .query_all(
            "SELECT a.file_path, a.job_id, a.action, a.created_at
               FROM job_file_activity a JOIN jobs j ON j.id = a.job_id
              WHERE j.project_id = ?1 AND a.created_at >= ?2 AND a.created_at <= ?3
              ORDER BY a.created_at DESC
              LIMIT 500",
            params![
                project_id.to_string(),
                (since_ms + 999).div_euclid(1000),
                until_ms.div_euclid(1000)
            ],
            |row| {
                Ok(TouchRow {
                    file_path: row.text(0)?,
                    job_id: row.text(1)?,
                    action: row.text(2)?,
                    ts_ms: row.i64(3)? * 1_000,
                })
            },
        )
        .await?;

    Ok(MapPresenceView { presence, touches })
}
