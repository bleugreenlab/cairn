//! Persistence boundary between the rollout schedule and executor directives.

use std::collections::HashMap;
use std::sync::Mutex;

use cairn_common::identity::PrincipalRef;
use cairn_config::drain::{
    AdmissionEffect, DrainOwner, RuntimeDrainContext, RuntimeDrainKind, RuntimeDrainToken,
};

use super::{management, rollout::SelfUpdateDrain, FleetConfig};
use crate::orchestrator::Orchestrator;

pub(super) struct SelfUpdateRuntime {
    pub(super) pass: Mutex<()>,
    boot_id: String,
    tokens: Mutex<HashMap<String, RuntimeDrainToken>>,
}

impl Default for SelfUpdateRuntime {
    fn default() -> Self {
        Self {
            pass: Mutex::new(()),
            boot_id: uuid::Uuid::new_v4().to_string(),
            tokens: Mutex::new(HashMap::new()),
        }
    }
}

/// Commit the proposed owned holds before applying the schedule or sending a
/// directive. The pass gate excludes a second proposal through application;
/// connection and rollout locks are never held across a settings write.
pub fn advance(orch: &Orchestrator, now_unix_ms: u64) -> Result<(), String> {
    if !orch.fleet.drain_projection_initialized() {
        return Err(match orch.fleet.drain_initialization_error() {
            Some(error) => format!("fleet initialization remains closed: {error}"),
            None => "fleet authoritative settings have not been initialized".into(),
        });
    }
    orch.fleet
        .advance_self_update_rollout(now_unix_ms, |required| {
            persist_required(orch, required, now_unix_ms)
        })
}

/// Commit the holds one pass requires. Runs under the pass gate, before the
/// schedule applies or any directive is sent. When the committed projection
/// already holds exactly the remembered tokens, the write would change
/// nothing, so the pass touches neither the settings file nor the projection.
fn persist_required(
    orch: &Orchestrator,
    required: &[SelfUpdateDrain],
    now_unix_ms: u64,
) -> Result<(), String> {
    let runtime = &orch.fleet.self_update_runtime;
    let principal = PrincipalRef::Machine {
        device_id: orch.anon_device_manager.device_id().to_string(),
    };
    let tokens = runtime.tokens.lock().unwrap().clone();
    if orch.fleet.with_committed_drain_records(|records| {
        holds_exact(records, required, &runtime.boot_id, &principal, &tokens)
    }) {
        return Ok(());
    }
    let next = management::mutate_fleet(orch, |config| {
        apply_required_drains(
            config,
            required,
            &runtime.boot_id,
            &principal,
            now_unix_ms
                .try_into()
                .map_err(|_| "self-update timestamp overflow")?,
            &tokens,
        )
    })?;
    *runtime.tokens.lock().unwrap() = next;
    Ok(())
}

fn self_update_context(hold: &SelfUpdateDrain, boot_id: &str) -> RuntimeDrainContext {
    RuntimeDrainContext {
        kind: RuntimeDrainKind::SelfUpdate,
        operation_id: hold.operation_id.clone(),
        turn_id: hold.operation_id.clone(),
        runner_boot_id: boot_id.to_string(),
        admission_effect: AdmissionEffect::RefuseNewEnvironments,
    }
}

fn self_update_reason(hold: &SelfUpdateDrain) -> String {
    format!(
        "self-update to {}; completion estimate unknown",
        hold.build_id
    )
}

/// Whether [`apply_required_drains`] would leave `records` untouched and
/// return `tokens` again: one remembered token per required hold, each naming
/// a committed record of the same owner, generation, reason and principal
/// (the impose no-op condition), and no other self-update record of this boot
/// standing anywhere. Anything else takes the full serialized write.
fn holds_exact(
    records: &HashMap<String, Vec<cairn_config::drain::DrainRecord>>,
    required: &[SelfUpdateDrain],
    boot_id: &str,
    principal: &PrincipalRef,
    tokens: &HashMap<String, RuntimeDrainToken>,
) -> bool {
    if tokens.len() != required.len() {
        return false;
    }
    let held = required.iter().all(|hold| {
        let Some(token) = tokens.get(&hold.executor_id) else {
            return false;
        };
        let context = self_update_context(hold, boot_id);
        token.context == context
            && token.drain_key == FleetConfig::drain_key(&hold.executor_id)
            && records.get(&hold.executor_id).is_some_and(|records| {
                records.iter().any(|record| {
                    record.owner
                        == DrainOwner::Runtime {
                            context: context.clone(),
                        }
                        && record.generation == token.generation
                        && record.reason == self_update_reason(hold)
                        && record.imposed_by() == Some(principal)
                })
            })
    });
    held && records.iter().all(|(executor_id, records)| {
        records.iter().all(|record| match &record.owner {
            DrainOwner::Runtime { context }
                if context.kind == RuntimeDrainKind::SelfUpdate
                    && context.runner_boot_id == boot_id =>
            {
                tokens.get(executor_id).is_some_and(|token| {
                    token.context == *context && token.generation == record.generation
                })
            }
            _ => true,
        })
    })
}

/// A new runner has no applied rollout turns. Retire only exact prior-boot
/// self-update tokens before resuming; other runtime kinds are not ours.
pub(super) fn recover_orphans(
    config: &mut FleetConfig,
    fleet: &super::Fleet,
) -> Result<(), String> {
    for (executor_id, records) in config.drain_records_executor_ids() {
        for record in records {
            let DrainOwner::Runtime { context } = record.owner else {
                continue;
            };
            if context.kind == RuntimeDrainKind::SelfUpdate
                && context.runner_boot_id != fleet.self_update_runtime.boot_id
            {
                config.drains.release_runtime(&RuntimeDrainToken {
                    drain_key: FleetConfig::drain_key(&executor_id).to_string(),
                    executor_id: executor_id.clone(),
                    context,
                    generation: record.generation,
                })?;
            }
        }
    }
    Ok(())
}

fn apply_required_drains(
    config: &mut FleetConfig,
    required: &[SelfUpdateDrain],
    boot_id: &str,
    principal: &PrincipalRef,
    now_unix_ms: i64,
    tokens: &HashMap<String, RuntimeDrainToken>,
) -> Result<HashMap<String, RuntimeDrainToken>, String> {
    // Compare every remembered token before making any change. In particular,
    // replacement of this runtime's own record is not permission to clear it.
    for token in tokens.values() {
        let current = config
            .drains
            .records(&token.executor_id)
            .into_iter()
            .find(|record| {
                record.owner
                    == DrainOwner::Runtime {
                        context: token.context.clone(),
                    }
            });
        if current.is_some_and(|record| record.generation != token.generation) {
            return Err(format!(
                "self-update drain token for {} was replaced; no rollout effect applied",
                token.executor_id
            ));
        }
    }
    for token in tokens.values() {
        if !required.iter().any(|hold| {
            hold.executor_id == token.executor_id && hold.operation_id == token.context.operation_id
        }) {
            config.drains.release_runtime(token)?;
        }
    }
    let mut next = HashMap::new();
    for hold in required {
        let token = config.drains.impose_runtime(
            &hold.executor_id,
            self_update_context(hold, boot_id),
            principal,
            &self_update_reason(hold),
            now_unix_ms,
        )?;
        next.insert(hold.executor_id.clone(), token);
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rollout_cannot_open_an_uninitialized_fleet_projection() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::LocalDb;
        use crate::storage::SearchIndex;
        use std::sync::Arc;
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
        // Fixtures otherwise start initialized; emulate the untouched startup
        // gate, before any load failure has supplied an error diagnostic.
        {
            let mut projection = orch.fleet.drain_projection.lock().unwrap();
            projection.initialized = false;
            projection.initialization_error = None;
        }
        assert!(orch.fleet.drain_initialization_error().is_none());
        assert!(advance(&orch, 1_700_000_000_000).is_err());
        assert!(!orch.fleet.drain_projection_initialized());
        assert!(!temp.path().join("settings.yaml").exists());
    }

    /// A stable pass reads the committed projection and stops: the settings
    /// file is not read (a corrupt one is not noticed) and nothing republishes.
    /// A changed requirement, a lost record or an orphan takes the full write.
    #[tokio::test]
    async fn a_stable_pass_writes_and_publishes_nothing() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::LocalDb;
        use crate::storage::SearchIndex;
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open_migrated_for_test(temp.path().join("stable.db"))
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
        let settings = temp.path().join("settings.yaml");
        let revision = || {
            orch.fleet
                .drain_projection
                .lock()
                .unwrap()
                .view("remote-id")
                .machine_revision
        };

        persist_required(&orch, &desired("build-a"), 1).unwrap();
        let first = revision();
        assert_eq!(orch.fleet.owned_drain_records("remote-id").len(), 1);
        let committed = std::fs::read_to_string(&settings).unwrap();

        // Prove the stable pass never opens the file: it would refuse this.
        std::fs::write(&settings, "buildSlots: [not, a, fleet").unwrap();
        persist_required(&orch, &desired("build-a"), 2).unwrap();
        persist_required(&orch, &desired("build-a"), 3).unwrap();
        assert_eq!(
            std::fs::read_to_string(&settings).unwrap(),
            "buildSlots: [not, a, fleet"
        );
        assert_eq!(revision(), first);
        // A changed requirement is a real write, so it now meets the file.
        assert!(persist_required(&orch, &desired("build-b"), 4).is_err());

        std::fs::write(&settings, committed).unwrap();
        persist_required(&orch, &desired("build-b"), 5).unwrap();
        assert!(revision() > first);
        let records = orch.fleet.owned_drain_records("remote-id");
        assert_eq!(records.len(), 1);
        assert!(records[0].reason.contains("build-b"));

        persist_required(&orch, &[], 6).unwrap();
        assert!(orch.fleet.owned_drain_records("remote-id").is_empty());
        assert!(orch
            .fleet
            .self_update_runtime
            .tokens
            .lock()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn exactness_requires_every_token_committed_and_no_orphan() {
        let principal = principal();
        let mut config = FleetConfig::default();
        let tokens = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            1,
            &HashMap::new(),
        )
        .unwrap();
        let records = |config: &FleetConfig| -> HashMap<_, _> {
            config.drain_records_executor_ids().into_iter().collect()
        };
        let exact = |config: &FleetConfig, required: &[SelfUpdateDrain], tokens: &HashMap<_, _>| {
            holds_exact(&records(config), required, "boot", &principal, tokens)
        };
        assert!(exact(&config, &desired("build-a"), &tokens));
        assert!(!exact(&config, &desired("build-b"), &tokens));
        assert!(!exact(&config, &[], &tokens));
        assert!(!exact(&config, &desired("build-a"), &HashMap::new()));
        assert!(!holds_exact(
            &records(&config),
            &desired("build-a"),
            "boot",
            &PrincipalRef::Machine {
                device_id: "other".into()
            },
            &tokens
        ));
        // Nothing required and nothing remembered, but this boot's hold stands.
        assert!(!exact(&config, &[], &HashMap::new()));

        let mut orphaned = config.clone();
        let mut context = tokens["remote-id"].context.clone();
        context.operation_id = "stale-turn".into();
        context.turn_id = "stale-turn".into();
        orphaned
            .drains
            .impose_runtime("other-id", context, &principal, "stale", 2)
            .unwrap();
        assert!(!exact(&orphaned, &desired("build-a"), &tokens));

        let mut lost = config.clone();
        lost.drains.release_runtime(&tokens["remote-id"]).unwrap();
        assert!(!exact(&lost, &desired("build-a"), &tokens));
        // Other owners and other boots' records are not this pass's concern.
        let mut shared = config.clone();
        shared
            .drains
            .impose_thread("remote-id", "thread-a", &principal, "maintenance", 3)
            .unwrap();
        apply_required_drains(
            &mut shared,
            &desired("build-a"),
            "prior-boot",
            &principal,
            4,
            &HashMap::new(),
        )
        .unwrap();
        assert!(exact(&shared, &desired("build-a"), &tokens));
    }

    fn principal() -> PrincipalRef {
        PrincipalRef::Machine {
            device_id: "runner".into(),
        }
    }

    fn desired(build: &str) -> Vec<SelfUpdateDrain> {
        vec![SelfUpdateDrain {
            executor_id: "remote-id".into(),
            operation_id: "turn".into(),
            build_id: build.into(),
        }]
    }

    #[test]
    fn retries_are_stable_and_release_preserves_other_owners() {
        let mut config = FleetConfig::default();
        let principal = principal();
        config
            .drains
            .impose_thread("remote-id", "thread-a", &principal, "maintenance", 1)
            .unwrap();
        let tokens = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            2,
            &HashMap::new(),
        )
        .unwrap();
        let revision = config.drains.machine_revision("remote-id");
        let retry = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            3,
            &tokens,
        )
        .unwrap();
        assert_eq!(tokens, retry);
        assert_eq!(config.drains.machine_revision("remote-id"), revision);
        let retargeted = apply_required_drains(
            &mut config,
            &desired("build-b"),
            "boot",
            &principal,
            4,
            &tokens,
        )
        .unwrap();
        assert!(retargeted["remote-id"].generation > tokens["remote-id"].generation);
        assert!(!config.drains.release_runtime(&tokens["remote-id"]).unwrap());
        apply_required_drains(&mut config, &[], "boot", &principal, 5, &retargeted).unwrap();
        let remaining = config.drains.records("remote-id");
        assert_eq!(remaining.len(), 1);
        assert!(
            matches!(&remaining[0].owner, DrainOwner::Thread { thread_id } if thread_id == "thread-a")
        );
    }

    #[test]
    fn a_replaced_runtime_record_is_neither_released_nor_overwritten() {
        let mut config = FleetConfig::default();
        let principal = principal();
        let tokens = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            1,
            &HashMap::new(),
        )
        .unwrap();
        let replacement = config
            .drains
            .impose_runtime(
                "remote-id",
                tokens["remote-id"].context.clone(),
                &principal,
                "replacement reason",
                2,
            )
            .unwrap();
        let before = config.drains.clone();
        assert!(apply_required_drains(&mut config, &[], "boot", &principal, 3, &tokens).is_err());
        assert_eq!(config.drains, before);
        assert!(apply_required_drains(
            &mut config,
            &desired("build-a"),
            "boot",
            &principal,
            4,
            &tokens
        )
        .is_err());
        assert_eq!(
            config.drains.records("remote-id")[0].generation,
            replacement.generation
        );
    }

    #[test]
    fn startup_recovers_only_prior_boot_self_update_tokens() {
        let mut config = FleetConfig::default();
        let principal = principal();
        let fleet = super::super::Fleet::default();
        let old = apply_required_drains(
            &mut config,
            &desired("build-a"),
            "prior-boot",
            &principal,
            1,
            &HashMap::new(),
        )
        .unwrap();
        let mut quiesce_context = old["remote-id"].context.clone();
        quiesce_context.kind = RuntimeDrainKind::Quiesce;
        let quiesce = config
            .drains
            .impose_runtime("remote-id", quiesce_context, &principal, "quiesce", 2)
            .unwrap();
        config
            .drains
            .impose_operator("remote-id", &principal, "operator maintenance", 3)
            .unwrap();
        let mut current = old["remote-id"].context.clone();
        current.runner_boot_id = fleet.self_update_runtime.boot_id.clone();
        current.operation_id = "current-turn".into();
        current.turn_id = "current-turn".into();
        let current = config
            .drains
            .impose_runtime("remote-id", current, &principal, "current self-update", 4)
            .unwrap();
        recover_orphans(&mut config, &fleet).unwrap();
        let remaining = config.drains.records("remote-id");
        assert_eq!(remaining.len(), 3);
        assert!(!remaining
            .iter()
            .any(|record| record.generation == old["remote-id"].generation));
        assert!(remaining
            .iter()
            .any(|record| record.generation == quiesce.generation));
        assert!(remaining
            .iter()
            .any(|record| record.generation == current.generation));
        let revision = config.drains.machine_revision("remote-id");
        recover_orphans(&mut config, &fleet).unwrap();
        assert_eq!(config.drains.machine_revision("remote-id"), revision);
    }
}
