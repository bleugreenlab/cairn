use super::{DbResult, LocalDb, RowExt};
use crate::turso::params;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorDesktopAutomation {
    pub executor_id: String,
    pub probed_at: i64,
    pub health_json: Option<String>,
    pub verbs_json: String,
    pub probe_error: Option<String>,
}

pub async fn get_executor_desktop_automation(
    db: &LocalDb,
    executor_id: &str,
) -> DbResult<Option<ExecutorDesktopAutomation>> {
    db.query_opt("SELECT executor_id, probed_at, health_json, verbs_json, probe_error FROM executor_desktop_automation WHERE executor_id = ?1", (executor_id.to_string(),), |row| Ok(ExecutorDesktopAutomation { executor_id: row.text(0)?, probed_at: row.i64(1)?, health_json: row.opt_text(2)?, verbs_json: row.text(3)?, probe_error: row.opt_text(4)? })).await
}

pub async fn upsert_executor_desktop_automation(
    db: &LocalDb,
    state: &ExecutorDesktopAutomation,
) -> DbResult<()> {
    db.execute("INSERT INTO executor_desktop_automation (executor_id, probed_at, health_json, verbs_json, probe_error) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(executor_id) DO UPDATE SET probed_at=excluded.probed_at, health_json=excluded.health_json, verbs_json=excluded.verbs_json, probe_error=excluded.probe_error", params![state.executor_id.clone(), state.probed_at, state.health_json.clone(), state.verbs_json.clone(), state.probe_error.clone()]).await?;
    Ok(())
}

pub async fn delete_executor_desktop_automation(db: &LocalDb, executor_id: &str) -> DbResult<bool> {
    Ok(db
        .execute(
            "DELETE FROM executor_desktop_automation WHERE executor_id = ?1",
            (executor_id,),
        )
        .await?
        > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;
    #[tokio::test]
    async fn cache_distinguishes_absent_empty_and_failed_probe() {
        let db = migrated_test_db("desktop-automation-cache.db").await;
        assert_eq!(
            get_executor_desktop_automation(&db, "e").await.unwrap(),
            None
        );
        let mut state = ExecutorDesktopAutomation {
            executor_id: "e".into(),
            probed_at: 10,
            health_json: Some("{}".into()),
            verbs_json: "[]".into(),
            probe_error: None,
        };
        upsert_executor_desktop_automation(&db, &state)
            .await
            .unwrap();
        assert_eq!(
            get_executor_desktop_automation(&db, "e").await.unwrap(),
            Some(state.clone())
        );
        state.probed_at = 20;
        state.health_json = None;
        state.probe_error = Some("offline".into());
        upsert_executor_desktop_automation(&db, &state)
            .await
            .unwrap();
        assert_eq!(
            get_executor_desktop_automation(&db, "e").await.unwrap(),
            Some(state)
        );
        assert!(delete_executor_desktop_automation(&db, "e").await.unwrap());
    }
}
