//! Fail-closed fleet settings initialization and its existing operator hold.

use std::path::PathBuf;
use std::sync::{Arc, Weak};

use super::{Fleet, FleetConfig};
use crate::notifications::{ExpectedRelease, Hold, HoldSource};
use crate::orchestrator::Orchestrator;
use crate::services::EventEmitter;

fn announce(emitter: &dyn EventEmitter) {
    let _ = emitter.emit(
        "db-change",
        serde_json::json!({
            "table": crate::notifications::HOLDS_TABLE,
        }),
    );
}

fn load(
    config_dir: &std::path::Path,
    fleet: &Fleet,
    emitter: &dyn EventEmitter,
) -> Result<FleetConfig, String> {
    let result = cairn_config::settings::mutate_fleet_committed(
        config_dir,
        |config| {
            super::self_update::recover_orphans(config, fleet)?;
            Ok(config.clone())
        },
        |config| {
            fleet.hydrate_machine_roles(config.role_executor_ids());
            fleet.publish_drain_projection(config);
        },
    );
    match result {
        Ok(committed) => {
            if let Some(warning) = committed.durability_warning {
                log::warn!("fleet settings committed with durability diagnostic: {warning}");
            }
            announce(emitter);
            Ok(committed.value)
        }
        Err(error) => {
            fleet.set_drain_initialization_error(error.clone());
            announce(emitter);
            Err(error)
        }
    }
}

/// An unreadable fleet is never an empty fleet. The caller may keep serving the
/// UI while retrying this controlled load; admission remains closed meanwhile.
pub fn load_authoritative(orch: &Orchestrator) -> Result<FleetConfig, String> {
    load(
        &orch.config_dir,
        &orch.fleet,
        orch.services.emitter.as_ref(),
    )
}

/// Initialize admission for every host, including servers with no artifact
/// plane or remote lifecycle manager. Failed loads do not block the UI and
/// remain closed while one controlled retry loop owns recovery.
pub fn start(orch: &Orchestrator) {
    if load_authoritative(orch).is_ok() {
        return;
    }
    let orch = orch.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(5),
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if load_authoritative(&orch).is_ok() {
                return;
            }
        }
    });
}

struct InitializationSource {
    fleet: Weak<Fleet>,
    config_dir: PathBuf,
    emitter: Arc<dyn EventEmitter>,
    opened_at: i64,
}

impl HoldSource for InitializationSource {
    fn name(&self) -> &'static str {
        "fleet-settings"
    }

    fn holds(&self, _project_id: &str) -> Vec<Hold> {
        let Some(fleet) = self.fleet.upgrade() else {
            return Vec::new();
        };
        let Some(error) = fleet.drain_initialization_error() else {
            return Vec::new();
        };
        vec![Hold {
            id: "fleet-settings-initialization".into(),
            project_id: None,
            reason: format!("Fleet admission is closed because authoritative settings could not be loaded: {error}"),
            expected_release: Some(ExpectedRelease::Text("After fleet settings are readable and valid; automatic retries remain active".into())),
            override_label: "Retry fleet settings".into(),
            source_uri: Some("cairn://executors".into()),
            opened_at: self.opened_at,
            released_at: None,
        }]
    }

    fn override_hold(&self, hold_id: &str) -> Result<(), String> {
        if hold_id != "fleet-settings-initialization" {
            return Err("unknown fleet initialization hold".into());
        }
        let fleet = self.fleet.upgrade().ok_or("fleet runner has stopped")?;
        load(&self.config_dir, &fleet, self.emitter.as_ref()).map(|_| ())
    }
}

/// Register the fleet's load failure with the notification center, not a second
/// drain or notice store. Weak ownership avoids retaining the orchestrator.
pub(crate) fn register(orch: &Orchestrator) {
    orch.register_hold_source(Arc::new(InitializationSource {
        fleet: Arc::downgrade(&orch.fleet),
        config_dir: orch.config_dir.clone(),
        emitter: orch.services.emitter.clone(),
        opened_at: orch.services.clock.now(),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::CapturingEmitter;

    #[tokio::test]
    async fn shared_start_retries_without_an_artifact_plane_or_remote_manager() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::LocalDb;
        use crate::storage::SearchIndex;
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open_migrated_for_test(temp.path().join("startup.db"))
                .await
                .unwrap(),
        );
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db, search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().to_path_buf(),
        )
        .build();
        {
            let mut projection = orch.fleet.drain_projection.lock().unwrap();
            projection.initialized = false;
            projection.initialization_error = None;
        }
        assert!(orch
            .fleet
            .executor_artifact_builds
            .lock()
            .unwrap()
            .is_empty());
        std::fs::write(temp.path().join("settings.yaml"), "buildSlots: [").unwrap();
        tokio::time::pause();
        start(&orch);
        assert!(!orch.fleet.drain_projection_initialized());
        assert!(orch.fleet.drain_initialization_error().is_some());
        tokio::task::yield_now().await;
        std::fs::write(temp.path().join("settings.yaml"), "buildSlots: {}\n").unwrap();
        assert!(cairn_config::settings::read_fleet_strict(temp.path()).is_ok());
        // Advancing the clock makes the retry runnable, but does not guarantee
        // that its task has completed before this task is scheduled again.
        let recovered = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !orch.fleet.drain_projection_initialized() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(
            recovered.is_ok(),
            "authoritative retry did not recover: {:?}",
            orch.fleet.drain_initialization_error()
        );
        assert!(orch.fleet.drain_initialization_error().is_none());
        assert!(!orch.fleet.is_drained(super::super::COLOCATED_EXECUTOR_ID));
    }

    #[test]
    fn malformed_settings_close_admission_and_surface_an_operator_hold_until_repaired() {
        let temp = tempfile::tempdir().unwrap();
        let fleet = Arc::new(Fleet::default());
        let emitter = Arc::new(CapturingEmitter::new());
        let source = InitializationSource {
            fleet: Arc::downgrade(&fleet),
            config_dir: temp.path().into(),
            emitter: emitter.clone(),
            opened_at: 1,
        };
        std::fs::write(temp.path().join("settings.yaml"), "buildSlots: [").unwrap();
        assert!(load(temp.path(), &fleet, emitter.as_ref()).is_err());
        assert!(fleet.is_drained(super::super::COLOCATED_EXECUTOR_ID));
        let holds = source.holds("project");
        assert_eq!(holds.len(), 1);
        assert!(holds[0].reason.contains("could not be loaded"));
        assert_eq!(holds[0].source_uri.as_deref(), Some("cairn://executors"));
        assert!(emitter
            .events_named("db-change")
            .iter()
            .any(|event| event["table"] == crate::notifications::HOLDS_TABLE));
        assert!(source.override_hold("unrelated").is_err());
        assert!(source
            .override_hold("fleet-settings-initialization")
            .is_err());
        assert!(fleet.is_drained("detached-id"));
        std::fs::write(temp.path().join("settings.yaml"), "buildSlots: {}\n").unwrap();
        source
            .override_hold("fleet-settings-initialization")
            .unwrap();
        assert!(source.holds("project").is_empty());
        assert!(fleet.drain_initialization_error().is_none());
        assert!(!fleet.is_drained(super::super::COLOCATED_EXECUTOR_ID));
    }
}
