//! Durable Claude CLI alias observations, cached without a second registry.

use std::collections::BTreeMap;
use std::time::Duration;

use cairn_db::storage::events::columns::{event_from_row, EVENT_COLUMNS};
use cairn_db::storage::events::reconstruct_events;
use cairn_db::storage::{run_db_blocking, DbResult, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_models::interface::AliasResolution;

use super::Orchestrator;

const CACHE_KEY: &str = "workspace";
// Replicated writes can arrive without this process's transcript callback.
const OBSERVATION_CACHE_AGE: Duration = Duration::from_secs(30);

async fn init_version(db: &LocalDb, run_id: &str, sequence: i64) -> DbResult<Option<String>> {
    let init = db
        .query_opt(
            format!(
                "SELECT {EVENT_COLUMNS} FROM events
                 WHERE run_id = ?1 AND sequence <= ?2 AND event_type = 'system:init'
                   AND parent_tool_use_id IS NULL
                 ORDER BY sequence DESC LIMIT 1"
            ),
            params![run_id, sequence],
            event_from_row,
        )
        .await?;
    let Some(init) = init else { return Ok(None) };
    let data = reconstruct_events(db, vec![init]).await.remove(0).data;
    Ok(reported_cli_version(&data))
}

fn reported_cli_version(data: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(data)
        .ok()
        .and_then(|data| {
            data.pointer("/raw/claude_code_version")
                .or_else(|| data.get("claude_code_version"))
                .and_then(|version| version.as_str())
                .filter(|version| !version.is_empty())
                .map(str::to_string)
        })
}

type ObservationOrder = (i64, i64, i64, String);

struct Observation {
    resolution: AliasResolution,
    order: ObservationOrder,
}

const LATEST_ALIAS_REPORT: &str =
    "SELECT e.reported_model, e.timestamp, s.id, e.run_id, e.sequence, e.created_at, e.id
 FROM events e JOIN sessions s ON s.id = e.session_id
 WHERE e.selected_model = ?1 AND s.backend COLLATE NOCASE = 'claude'
   AND e.event_type = 'system:init' AND e.parent_tool_use_id IS NULL
   AND e.reported_model IS NOT NULL AND e.reported_model <> ''
 ORDER BY e.timestamp DESC, e.created_at DESC, e.sequence DESC, e.id DESC LIMIT 1";

async fn observations(db: &LocalDb) -> DbResult<Vec<Observation>> {
    let mut resolutions = Vec::new();
    for alias in ["haiku", "sonnet", "opus", "fable"] {
        let latest = db
            .query_opt(LATEST_ALIAS_REPORT, params![alias], |row| {
                Ok((
                    row.text(0)?,
                    row.i64(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.i64(4)?,
                    row.i64(5)?,
                    row.text(6)?,
                ))
            })
            .await?;
        if let Some((reported, observed_at, session_id, run_id, sequence, created_at, event_id)) =
            latest
        {
            resolutions.push(Observation {
                order: (observed_at, created_at, sequence, event_id),
                resolution: AliasResolution {
                    alias: alias.to_string(),
                    canonical: cairn_models::types::identity::for_offer("claude", &reported, None)
                        .map(str::to_string),
                    reported,
                    observed_at,
                    session_id,
                    cli_version: init_version(db, &run_id, sequence).await?,
                    current_cli_version: None,
                },
            });
        }
    }
    Ok(resolutions)
}

async fn latest_cli_version(db: &LocalDb) -> DbResult<Option<(ObservationOrder, String)>> {
    let columns = EVENT_COLUMNS
        .split(',')
        .map(|column| format!("e.{}", column.trim()))
        .collect::<Vec<_>>()
        .join(", ");
    let mut offset = 0;
    loop {
        let events = db
            .query_all(
                format!(
                    "SELECT {columns}
                     FROM events e JOIN sessions s ON s.id = e.session_id
                     WHERE s.backend COLLATE NOCASE = 'claude'
                       AND e.event_type = 'system:init' AND e.parent_tool_use_id IS NULL
                     ORDER BY e.timestamp DESC, e.created_at DESC, e.sequence DESC, e.id DESC
                     LIMIT 64 OFFSET ?1"
                ),
                params![offset],
                event_from_row,
            )
            .await?;
        let count = events.len();
        if count == 0 {
            return Ok(None);
        }
        for event in reconstruct_events(db, events).await {
            if let Some(version) = reported_cli_version(&event.data) {
                return Ok(Some((
                    (
                        event.timestamp,
                        event.created_at,
                        i64::from(event.sequence),
                        event.id,
                    ),
                    version,
                )));
            }
        }
        offset += count as i64;
    }
}

impl Orchestrator {
    pub(crate) fn claude_alias_resolutions(&self) -> Vec<AliasResolution> {
        run_db_blocking({
            let orch = self.clone();
            move || async move {
                Ok(orch
                    .claude_alias_cache
                    .get_or_compute_within(CACHE_KEY, OBSERVATION_CACHE_AGE, || async {
                        match orch.read_claude_alias_resolutions().await {
                            Ok(resolutions) => Some(resolutions),
                            Err(error) => {
                                log::warn!("Could not read Claude alias observations: {error}");
                                None
                            }
                        }
                    })
                    .await
                    .unwrap_or_default())
            }
        })
        .unwrap_or_else(|error| {
            log::warn!("Could not read Claude alias observation cache: {error}");
            Vec::new()
        })
    }

    async fn read_claude_alias_resolutions(&self) -> Result<Vec<AliasResolution>, String> {
        let mut latest = BTreeMap::<String, Observation>::new();
        let mut version: Option<(ObservationOrder, String)> = None;
        for db in self.db.all_dbs().await {
            for observation in observations(&db).await.map_err(|error| error.to_string())? {
                let replace = latest
                    .get(&observation.resolution.alias)
                    .is_none_or(|old| observation.order > old.order);
                if replace {
                    latest.insert(observation.resolution.alias.clone(), observation);
                }
            }
            if let Some(candidate) = latest_cli_version(&db)
                .await
                .map_err(|error| error.to_string())?
            {
                if version.as_ref().is_none_or(|old| candidate > *old) {
                    version = Some(candidate);
                }
            }
        }
        Ok(latest
            .into_values()
            .map(|observation| {
                let mut resolution = observation.resolution;
                resolution.current_cli_version = version
                    .as_ref()
                    .filter(|(order, _)| *order >= observation.order)
                    .map(|(_, version)| version.clone());
                resolution
            })
            .collect())
    }

    /// Called only after an init report has durably committed.
    pub(crate) fn claude_alias_report_committed(&self) {
        self.claude_alias_cache
            .invalidate(CACHE_KEY, "session init reported");
        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "model_catalog"}),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::models::tests::{bare_orchestrator, seed_alias_session};

    async fn report(orch: &Orchestrator, name: &str, model: &str, at: i64, child: bool) {
        let child = if child { "'child'" } else { "NULL" };
        orch.db
            .local
            .execute_script(format!(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                                parent_tool_use_id, created_at, reported_model, selected_model)
             VALUES ('{name}-event', '{name}-run', '{name}-session', 1, {at}, 'system:init', '{{}}',
                     {child}, {at}, '{model}', (SELECT model FROM jobs WHERE id = '{name}-job'));"
            ))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn latest_report_wins_even_when_unknown_and_children_and_other_backends_do_not_count() {
        let orch = bare_orchestrator().await;
        for (name, model, backend) in [
            ("old", "opus", "claude"),
            ("new", "opus", "CLAUDE"),
            ("other", "opus", "codex"),
            ("child", "opus", "claude"),
            ("named", "claude-opus-5-5", "claude"),
        ] {
            seed_alias_session(&orch, name, model, backend).await;
        }
        report(&orch, "old", "claude-opus-4-7", 100, false).await;
        report(&orch, "new", "unlisted-claude-checkpoint", 101, false).await;
        report(&orch, "other", "claude-opus-5-5", 103, false).await;
        report(&orch, "child", "claude-opus-5-5", 104, true).await;
        report(&orch, "named", "claude-opus-5-5", 105, false).await;
        let plan = orch
            .db
            .local
            .query_all(
                format!("EXPLAIN QUERY PLAN {LATEST_ALIAS_REPORT}"),
                params!["opus"],
                |row| row.text(3),
            )
            .await
            .unwrap()
            .join("\n");
        assert!(plan.contains("idx_events_selected_model_init"), "{plan}");
        let resolutions = observations(&orch.db.local).await.unwrap();
        assert_eq!(resolutions.len(), 1);
        assert_eq!(resolutions[0].resolution.session_id, "new-session");
        assert_eq!(
            resolutions[0].resolution.reported,
            "unlisted-claude-checkpoint"
        );
        assert_eq!(resolutions[0].resolution.canonical, None);

        orch.db
            .local
            .execute("UPDATE jobs SET model = 'sonnet' WHERE id = 'new-job'", ())
            .await
            .unwrap();
        let after_edit = observations(&orch.db.local).await.unwrap();
        assert_eq!(after_edit.len(), 1);
        assert_eq!(after_edit[0].resolution.alias, "opus");
        assert_eq!(after_edit[0].resolution.session_id, "new-session");
        orch.db
            .local
            .execute("UPDATE events SET selected_model = NULL", ())
            .await
            .unwrap();
        assert!(
            observations(&orch.db.local).await.unwrap().is_empty(),
            "historical reports without immutable selection must not be guessed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn newer_cli_init_expires_an_alias_and_archived_init_preserves_version() {
        let orch = bare_orchestrator().await;
        seed_alias_session(&orch, "alias", "opus", "claude").await;
        seed_alias_session(&orch, "named", "claude-sonnet-5", "claude").await;
        let now = chrono::Utc::now().timestamp();
        orch.db.local.execute_script(format!(
            "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                                created_at, reported_model, selected_model)
             VALUES ('alias-init', 'alias-run', 'alias-session', 1, {now}, 'system:init',
                     '{{\"raw\":{{\"model\":\"claude-opus-4-7\",\"claude_code_version\":\"2.1.300\"}}}}',
                     {now}, 'claude-opus-4-7', 'opus'),
                    ('named-init', 'named-run', 'named-session', 1, {now}, 'system:init',
                     '{{\"raw\":{{\"model\":\"claude-sonnet-5\",\"claude_code_version\":\"2.1.311\"}}}}',
                     {now}, 'claude-sonnet-5', 'claude-sonnet-5'),
                    ('a-versionless-init', 'named-run', 'named-session', 2, {now}, 'system:init',
                     '{{}}', {now}, 'claude-sonnet-5', 'claude-sonnet-5');"
        )).await.unwrap();
        let resolutions = orch.read_claude_alias_resolutions().await.unwrap();
        assert_eq!(resolutions[0].cli_version.as_deref(), Some("2.1.300"));
        assert_eq!(
            resolutions[0].current_cli_version.as_deref(),
            Some("2.1.311")
        );
        assert_eq!(resolutions[0].canonical_at(now), None);
        assert!(resolutions[0]
            .provenance_at(now)
            .contains("CLI version changed"));

        let payload = br#"{"raw":{"model":"claude-opus-4-7","claude_code_version":"2.1.300"}}"#;
        let blob = cairn_db::storage::events::compress(payload).unwrap();
        orch.db
            .local
            .execute(
                "UPDATE events SET storage_mode = 'zstd', data = '{}', data_blob = ?1,
                               codec = ?2 WHERE id = 'alias-init'",
                params![blob, cairn_db::storage::events::CODEC_ZSTD_V1],
            )
            .await
            .unwrap();
        assert_eq!(
            init_version(&orch.db.local, "alias-run", 1)
                .await
                .unwrap()
                .as_deref(),
            Some("2.1.300")
        );
    }
}
