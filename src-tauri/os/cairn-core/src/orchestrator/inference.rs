//! The cairn-inference facet's host: the inventory and the bginf link row in
//! the orch's database, and the poller that mirrors bginf into them.
//!
//! The poller sends bginf exactly two GET requests per tick (`/profiles` and
//! `/state`) through the facet's GET-only client, and starts, stops or powers
//! nothing. Each tick re-imports bginf's configuration and reconciles it into
//! the records whose source is bginf; the raw responses are kept beside them so
//! the comparison is always computed from one snapshot.

use std::collections::BTreeMap;
use std::time::Duration;

use cairn_db::turso::params;
use cairn_inference::bginf::{self, BginfClient, MachineMap, Profiles, State, Topology};
use cairn_inference::{ImportReport, Inventory, RecordSource};
use serde::Serialize;

use super::Orchestrator;
use crate::credentials::{ApiProvider, ProviderAuth};
use crate::storage::{LocalDb, RowExt};
use cairn_config::inference::BginfLinkSettings;

/// How often the poller reads bginf.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30);

const RECIPES: &str = "inference_recipes";
const DEPLOYMENTS: &str = "inference_deployments";
const POOLS: &str = "inference_pools";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The bginf link's one row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LinkRow {
    pub last_attempt_at: Option<i64>,
    /// The last attempt's error; cleared by a successful read.
    pub last_error: Option<String>,
    /// When the current run of failed reads began.
    pub failing_since: Option<i64>,
    pub last_ok_at: Option<i64>,
    pub profiles_json: Option<String>,
    pub state_json: Option<String>,
    /// What the last successful import refused or could not map.
    pub last_warnings: Vec<String>,
    pub last_change_at: Option<i64>,
    pub last_change_report: Option<ImportReport>,
}

impl LinkRow {
    /// The last successful `/state`, parsed.
    pub fn state(&self) -> Option<State> {
        serde_json::from_str(self.state_json.as_deref()?).ok()
    }
}

async fn load_bodies(db: &LocalDb, table: &'static str) -> Result<Vec<(String, String)>, String> {
    db.query_all(
        format!("SELECT id, body_json FROM {table} ORDER BY id"),
        (),
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map_err(|error| format!("read {table}: {error}"))
}

fn parse_bodies<T: serde::de::DeserializeOwned>(
    table: &str,
    rows: Vec<(String, String)>,
) -> Vec<T> {
    rows.into_iter()
        .filter_map(|(id, body)| match serde_json::from_str(&body) {
            Ok(record) => Some(record),
            Err(error) => {
                log::warn!("inference: {table} row {id} does not parse and is skipped: {error}");
                None
            }
        })
        .collect()
}

/// The stored inventory.
pub async fn load_inventory(db: &LocalDb) -> Result<Inventory, String> {
    let mut inventory = Inventory::default();
    for recipe in parse_bodies(RECIPES, load_bodies(db, RECIPES).await?) {
        inventory.insert_recipe(recipe);
    }
    for deployment in parse_bodies(DEPLOYMENTS, load_bodies(db, DEPLOYMENTS).await?) {
        inventory.insert_deployment(deployment);
    }
    for pool in parse_bodies(POOLS, load_bodies(db, POOLS).await?) {
        inventory.insert_pool(pool);
    }
    Ok(inventory)
}

/// The bginf link row, if the poller has ever written it.
pub async fn load_link(db: &LocalDb) -> Result<Option<LinkRow>, String> {
    db.query_opt(
        "SELECT last_attempt_at, last_error, failing_since, last_ok_at, profiles_json, state_json,
                last_warnings_json, last_change_at, last_change_report_json
           FROM inference_bginf_link WHERE link = 'bginf'",
        (),
        |row| {
            Ok(LinkRow {
                last_attempt_at: row.opt_i64(0)?,
                last_error: row.opt_text(1)?,
                failing_since: row.opt_i64(2)?,
                last_ok_at: row.opt_i64(3)?,
                profiles_json: row.opt_text(4)?,
                state_json: row.opt_text(5)?,
                last_warnings: row
                    .opt_text(6)?
                    .and_then(|json| serde_json::from_str(&json).ok())
                    .unwrap_or_default(),
                last_change_at: row.opt_i64(7)?,
                last_change_report: row
                    .opt_text(8)?
                    .and_then(|json| serde_json::from_str(&json).ok()),
            })
        },
    )
    .await
    .map_err(|error| format!("read inference_bginf_link: {error}"))
}

/// One row to write: `body` `None` deletes it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowChange {
    table: &'static str,
    id: String,
    source: &'static str,
    body: Option<String>,
}

fn diff_kind<K: Ord + ToString, T: Serialize + PartialEq>(
    table: &'static str,
    current: &BTreeMap<K, T>,
    next: &BTreeMap<K, T>,
    source_of: fn(&T) -> RecordSource,
    changes: &mut Vec<RowChange>,
) {
    for (id, record) in next {
        if current.get(id) != Some(record) {
            changes.push(RowChange {
                table,
                id: id.to_string(),
                source: source_of(record).as_str(),
                body: Some(serde_json::to_string(record).expect("inference records serialize")),
            });
        }
    }
    for (id, record) in current {
        if !next.contains_key(id) {
            changes.push(RowChange {
                table,
                id: id.to_string(),
                source: source_of(record).as_str(),
                body: None,
            });
        }
    }
}

fn diff_rows(current: &Inventory, next: &Inventory) -> Vec<RowChange> {
    let mut changes = Vec::new();
    diff_kind(
        RECIPES,
        &current.recipes,
        &next.recipes,
        |r| r.source,
        &mut changes,
    );
    diff_kind(
        DEPLOYMENTS,
        &current.deployments,
        &next.deployments,
        |d| d.source,
        &mut changes,
    );
    diff_kind(
        POOLS,
        &current.pools,
        &next.pools,
        |p| p.source,
        &mut changes,
    );
    changes
}

/// What one successful read writes beside the records.
#[derive(Debug, Clone)]
struct SuccessfulRead {
    at: i64,
    profiles_json: String,
    state_json: String,
    warnings: Vec<String>,
    report: ImportReport,
}

async fn save_success(
    db: &LocalDb,
    changes: Vec<RowChange>,
    read: SuccessfulRead,
) -> Result<(), String> {
    let warnings = serde_json::to_string(&read.warnings).expect("warnings serialize");
    let (change_at, change_report) = if read.report.has_changes() {
        (
            Some(read.at),
            Some(serde_json::to_string(&read.report).expect("import reports serialize")),
        )
    } else {
        (None, None)
    };
    db.write(move |conn| {
        let changes = changes.clone();
        let read = read.clone();
        let warnings = warnings.clone();
        let change_report = change_report.clone();
        Box::pin(async move {
            for change in changes {
                match change.body {
                    Some(body) => {
                        conn.execute(
                            &format!(
                                "INSERT INTO {} (id, source, body_json, created_at, updated_at)
                                 VALUES (?1, ?2, ?3, ?4, ?4)
                                 ON CONFLICT(id) DO UPDATE SET source = excluded.source,
                                   body_json = excluded.body_json, updated_at = excluded.updated_at",
                                change.table
                            ),
                            params![change.id, change.source, body, read.at],
                        )
                        .await?;
                    }
                    None => {
                        conn.execute(
                            &format!("DELETE FROM {} WHERE id = ?1", change.table),
                            params![change.id],
                        )
                        .await?;
                    }
                }
            }
            conn.execute(
                "INSERT INTO inference_bginf_link
                   (link, last_attempt_at, last_error, failing_since, last_ok_at, profiles_json,
                    state_json, last_warnings_json, last_change_at, last_change_report_json)
                 VALUES ('bginf', ?1, NULL, NULL, ?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(link) DO UPDATE SET
                   last_attempt_at = excluded.last_attempt_at,
                   last_error = NULL,
                   failing_since = NULL,
                   last_ok_at = excluded.last_ok_at,
                   profiles_json = excluded.profiles_json,
                   state_json = excluded.state_json,
                   last_warnings_json = excluded.last_warnings_json,
                   last_change_at = COALESCE(excluded.last_change_at, inference_bginf_link.last_change_at),
                   last_change_report_json =
                     COALESCE(excluded.last_change_report_json, inference_bginf_link.last_change_report_json)",
                params![
                    read.at,
                    read.profiles_json,
                    read.state_json,
                    warnings,
                    change_at,
                    change_report
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("write inference records: {error}"))
}

async fn save_failure(db: &LocalDb, at: i64, error: String) -> Result<(), String> {
    db.execute(
        "INSERT INTO inference_bginf_link (link, last_attempt_at, last_error, failing_since)
         VALUES ('bginf', ?1, ?2, ?1)
         ON CONFLICT(link) DO UPDATE SET
           last_attempt_at = excluded.last_attempt_at,
           last_error = excluded.last_error,
           failing_since = COALESCE(inference_bginf_link.failing_since, excluded.failing_since)",
        params![at, error],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("write inference_bginf_link: {error}"))
}

/// Import one successful read and persist it with the records it changed.
/// Records that would leave the inventory invalid are not written; why goes
/// into the warnings.
async fn apply_read(
    db: &LocalDb,
    profiles: serde_json::Value,
    state: serde_json::Value,
    map: &MachineMap,
    at: i64,
) -> Result<ImportReport, String> {
    let parsed_profiles: Profiles = serde_json::from_value(profiles.clone())
        .map_err(|error| format!("GET /profiles: unexpected shape: {error}"))?;
    let parsed_state: State = serde_json::from_value(state.clone())
        .map_err(|error| format!("GET /state: unexpected shape: {error}"))?;
    let imported = bginf::import(&parsed_profiles, Topology::Live(&parsed_state), map);
    let current = load_inventory(db).await?;
    let (merged, mut report) =
        Inventory::reconcile(&current, &imported.inventory, RecordSource::Bginf);
    report.warnings = imported.warnings;
    let next = match merged.validate() {
        Ok(()) => merged,
        Err(problems) => {
            report
                .warnings
                .extend(problems.iter().map(|p| format!("not written: {p}")));
            report.recipes = Default::default();
            report.deployments = Default::default();
            report.pools = Default::default();
            current.clone()
        }
    };
    let changes = diff_rows(&current, &next);
    save_success(
        db,
        changes,
        SuccessfulRead {
            at,
            profiles_json: profiles.to_string(),
            state_json: state.to_string(),
            warnings: report.warnings.clone(),
            report: report.clone(),
        },
    )
    .await?;
    Ok(report)
}

/// The token of the first `bginf` account, if one is stored.
fn bginf_token(orch: &Orchestrator) -> Option<String> {
    let store = orch.get_credential_store()?;
    store
        .accounts_for_provider(ApiProvider::Bginf, None)
        .into_iter()
        .find_map(|account| match &account.auth {
            ProviderAuth::ApiKey { value } | ProviderAuth::OAuthToken { value } => {
                Some(value.clone())
            }
            _ => None,
        })
}

/// Whether the link is configured, and what is missing when it is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkConfig {
    Configured(BginfLinkSettings),
    Missing { setting: bool, account: bool },
}

pub fn link_config(orch: &Orchestrator) -> LinkConfig {
    let setting = crate::config::settings::load_inference(&orch.config_dir).bginf;
    let has_token = bginf_token(orch).is_some();
    match setting {
        Some(link) if has_token => LinkConfig::Configured(link),
        setting => LinkConfig::Missing {
            setting: setting.is_none(),
            account: !has_token,
        },
    }
}

pub fn machine_map(link: &BginfLinkSettings) -> MachineMap {
    MachineMap::new(link.machines.clone())
}

/// One poller tick. Does nothing while the link is not configured.
pub async fn poll_once(orch: &Orchestrator) {
    let LinkConfig::Configured(link) = link_config(orch) else {
        return;
    };
    let Some(token) = bginf_token(orch) else {
        return;
    };
    let at = now_ms();
    let url = link.url.clone();
    let fetched = tokio::task::spawn_blocking(move || {
        let client = BginfClient::new(url, token).map_err(|error| error.to_string())?;
        let profiles = client.get_profiles().map_err(|error| error.to_string())?;
        let state = client.get_state().map_err(|error| error.to_string())?;
        Ok::<_, String>((profiles, state))
    })
    .await
    .unwrap_or_else(|error| Err(format!("the bginf read did not finish: {error}")));
    let db = &orch.db.local;
    let outcome = match fetched {
        Ok((profiles, state)) => apply_read(db, profiles, state, &machine_map(&link), at).await,
        Err(error) => Err(error),
    };
    match outcome {
        Ok(report) if report.has_changes() => {
            log::info!(
                "inference: bginf import changed {}",
                report.lines().join("; ")
            );
        }
        Ok(_) => {}
        Err(error) => {
            if let Err(write) = save_failure(db, at, error.clone()).await {
                log::warn!(
                    "inference: bginf read failed ({error}) and could not be recorded: {write}"
                );
            }
        }
    }
}

/// Read bginf now and every [`POLL_INTERVAL`] for the life of the process.
pub fn spawn_bginf_poller(orch: Orchestrator) {
    tokio::spawn(async move {
        loop {
            poll_once(&orch).await;
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
}

/// Everything the `cairn://inference` pages render.
#[derive(Debug, Clone)]
pub struct InferenceView {
    pub config: LinkConfig,
    pub link: Option<LinkRow>,
    pub inventory: Inventory,
    /// Enrolled executors by name: `true` when attached now.
    pub executors: BTreeMap<String, bool>,
    pub now_ms: i64,
}

pub async fn load_view(orch: &Orchestrator) -> Result<InferenceView, String> {
    let captured_at = crate::fleet::unix_time_ms();
    let mut executors: BTreeMap<String, bool> = orch
        .fleet
        .unattached_enrolled_remotes()
        .into_iter()
        .map(|remote| (remote.name, false))
        .collect();
    for executor in orch.fleet.inspect_executors(captured_at) {
        executors.insert(executor.name, true);
    }
    Ok(InferenceView {
        config: link_config(orch),
        link: load_link(&orch.db.local).await?,
        inventory: load_inventory(&orch.db.local).await?,
        executors,
        now_ms: now_ms(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profiles() -> serde_json::Value {
        serde_json::json!({
            "qwen36-fast": {"id": "qwen36-fast", "nodes": ["sp1"], "head": "sp1",
                "start": "./start.sh", "stop": "./stop.sh",
                "health": "http://192.0.2.40:8001/v1/models", "api_base": "http://192.0.2.40:8001/v1"},
            "qwen3-embed-4b": {"id": "qwen3-embed-4b", "nodes": ["halo"], "head": "halo",
                "start": "./start.sh", "stop": "./stop.sh",
                "health": "http://192.0.2.43:8090/health", "api_base": "http://192.0.2.43:8090/v1"},
            "off": {"id": "off", "nodes": []}
        })
    }

    fn state() -> serde_json::Value {
        serde_json::json!({
            "pools": {
                "sparks": {"desired": "qwen36-fast", "reason": "default", "pin": null,
                           "default": "qwen36-fast", "running": null, "converged": false,
                           "nodes": ["sp1", "sp2"]},
                "halo": {"desired": "qwen3-embed-4b", "reason": "default", "pin": null,
                         "default": "qwen3-embed-4b", "running": null, "converged": false,
                         "nodes": ["halo"]}
            }
        })
    }

    fn halo_is_fedora() -> MachineMap {
        MachineMap::new([("halo".to_string(), "fedora".to_string())])
    }

    #[tokio::test]
    async fn a_read_persists_and_a_repeat_changes_nothing() {
        let db = crate::storage::migrated_test_db("inference-store").await;
        let report = apply_read(&db, profiles(), state(), &halo_is_fedora(), 1_000)
            .await
            .unwrap();
        assert!(report.has_changes());
        let stored = load_inventory(&db).await.unwrap();
        assert_eq!(stored.deployments.len(), 2);
        assert_eq!(
            stored.deployments[&"qwen3-embed-4b".into()].head,
            "fedora".into()
        );
        let link = load_link(&db).await.unwrap().unwrap();
        assert_eq!(link.last_ok_at, Some(1_000));
        assert_eq!(link.last_change_at, Some(1_000));
        assert!(link.state().is_some());

        let again = apply_read(&db, profiles(), state(), &halo_is_fedora(), 2_000)
            .await
            .unwrap();
        assert!(!again.has_changes(), "{again:?}");
        assert_eq!(load_inventory(&db).await.unwrap(), stored);
        let link = load_link(&db).await.unwrap().unwrap();
        assert_eq!(link.last_ok_at, Some(2_000));
        assert_eq!(
            link.last_change_at,
            Some(1_000),
            "a quiet read keeps the last change"
        );
    }

    #[tokio::test]
    async fn a_removed_profile_is_deleted_and_failures_keep_the_last_read() {
        let db = crate::storage::migrated_test_db("inference-store-remove").await;
        apply_read(&db, profiles(), state(), &halo_is_fedora(), 1_000)
            .await
            .unwrap();
        let mut fewer = profiles();
        fewer.as_object_mut().unwrap().remove("qwen36-fast");
        let report = apply_read(&db, fewer, state(), &halo_is_fedora(), 2_000)
            .await
            .unwrap();
        assert!(report
            .lines()
            .contains(&"removed deployment qwen36-fast".to_string()));
        assert!(!load_inventory(&db)
            .await
            .unwrap()
            .deployments
            .contains_key(&"qwen36-fast".into()));

        save_failure(&db, 3_000, "GET /state: connection refused".into())
            .await
            .unwrap();
        save_failure(&db, 4_000, "GET /state: connection refused".into())
            .await
            .unwrap();
        let link = load_link(&db).await.unwrap().unwrap();
        assert_eq!(link.failing_since, Some(3_000));
        assert_eq!(link.last_attempt_at, Some(4_000));
        assert_eq!(link.last_ok_at, Some(2_000));
        assert!(link.state().is_some());

        apply_read(&db, profiles(), state(), &halo_is_fedora(), 5_000)
            .await
            .unwrap();
        let link = load_link(&db).await.unwrap().unwrap();
        assert_eq!((link.last_error, link.failing_since), (None, None));
    }

    #[tokio::test]
    async fn a_malformed_response_is_a_failure_not_an_import() {
        let db = crate::storage::migrated_test_db("inference-store-shape").await;
        let error = apply_read(
            &db,
            serde_json::json!([1, 2]),
            state(),
            &halo_is_fedora(),
            1_000,
        )
        .await
        .unwrap_err();
        assert!(error.starts_with("GET /profiles"), "{error}");
        assert_eq!(load_inventory(&db).await.unwrap(), Inventory::default());
    }
}
