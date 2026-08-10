//! Resource-pack mutations: install, update, restore, uninstall, and removing
//! one item without the pack around it.
//!
//! All of them run the same machinery the startup sync runs, so a pack
//! installed from the catalog and a pack installed on a fresh run are the same
//! thing afterwards — same destination layout, same ownership rules, same lock.
//! Both callers reach these functions: the `cairn://packs/{id}` writes an agent
//! makes, and the typed settings commands. That shared entry is what makes the
//! registry event below unmissable rather than something each surface remembers
//! to send.

use serde::{Deserialize, Serialize};

use crate::config::pack::{self, PackItem, PackItemKind};
use crate::orchestrator::Orchestrator;
use crate::services::{RealFileSystem, RealGitClient};
use crate::workspace::bundle::{sync_one_pack, uninstall_pack};

/// What one pack mutation did, in the terms the catalog itself uses.
///
/// The human `summary` is what an agent's write reports; every other field is
/// the same facts in a form a settings screen can act on, so the two surfaces
/// cannot describe one action differently.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PackMutationResult {
    /// `install`, `update`, `restore`, `uninstall`, or `remove-item`.
    pub action: String,
    pub pack_id: String,
    /// Workspace-relative paths a sync wrote or retired.
    pub changed_paths: Vec<String>,
    /// Paths an update refused to overwrite because the user had made them
    /// theirs.
    pub skipped_conflicts: Vec<String>,
    /// Paths an uninstall deleted.
    pub removed_paths: Vec<String>,
    /// Paths an uninstall left in place because the user had edited them.
    pub kept_paths: Vec<String>,
    /// Items a restore brought back.
    pub restored_items: Vec<PackItem>,
    /// The single item a removal took out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_item: Option<PackItem>,
    pub summary: String,
}

/// The app resource directory this workspace syncs from, recorded by the last
/// startup sync. Without it there is nothing to install FROM.
fn source_dir(orch: &Orchestrator) -> Result<std::path::PathBuf, String> {
    pack::source_dir(&orch.config_dir).ok_or_else(|| {
        "This workspace has no recorded app resource directory yet, so no pack can be installed \
         from it. It is recorded on the next app startup."
            .to_string()
    })
}

/// Install, update, or restore one pack.
///
/// Emits the registry change only after the filesystem, lock, and Git work has
/// succeeded, so a failed action never invalidates a caller's view of a
/// workspace that did not move.
pub fn apply_pack_action(
    orch: &Orchestrator,
    action: &str,
    pack_id: &str,
) -> Result<PackMutationResult, String> {
    // Resolve what this action will sync FROM before touching any lock. A
    // restore records its intent by rewriting the pack lock, so a source that
    // could never have been synced from must be refused while that record is
    // still intact — otherwise the reachable "installed pack whose shipped
    // source is gone" case clears the user's removal markers and then fails.
    let resource_dir = source_dir(orch)?;
    if pack::available_pack(&resource_dir, pack_id).is_none() {
        return Err(format!(
            "No pack `{pack_id}` is shipped in {resource_dir:?}"
        ));
    }

    let previous_lock = pack::lock::read_lock(&orch.config_dir, pack_id);
    let installed = previous_lock.is_some();
    let mut restored_items: Vec<PackItem> = Vec::new();
    match action {
        "install" if installed => {
            return Err(format!(
                "Pack `{pack_id}` is already installed. Use action:\"update\" to re-sync it."
            ))
        }
        "update" | "restore" if !installed => {
            return Err(format!(
                "Pack `{pack_id}` is not installed. Use action:\"install\" first."
            ))
        }
        "restore" => {
            restored_items =
                pack::lock::restore_removed_items(&RealFileSystem, &orch.config_dir, pack_id)?;
        }
        "install" | "update" => {}
        other => {
            return Err(format!(
                "Unknown pack action `{other}` (install | update | restore)"
            ))
        }
    }

    let result = match sync_one_pack(
        &RealGitClient,
        &RealFileSystem,
        &resource_dir,
        &orch.config_dir,
        pack_id,
    ) {
        Ok(result) => result,
        Err(error) => {
            // The restore above already discarded the removal records; the sync
            // that was to materialize those items did not run. Put the records
            // back, so the failure leaves the workspace exactly as it was and
            // the user keeps the way to try again.
            if let (false, Some(previous)) = (restored_items.is_empty(), previous_lock) {
                if let Err(rollback) =
                    pack::lock::rewrite_lock(&RealFileSystem, &orch.config_dir, &previous)
                {
                    log::error!(
                        "Pack `{pack_id}`: restore failed ({error}) and its removal records could \
                         not be put back ({rollback}); {} item(s) are now neither installed nor \
                         recorded as removed",
                        restored_items.len()
                    );
                }
            }
            return Err(error);
        }
    };

    let mut summary = match action {
        "install" => format!("Installed pack '{pack_id}'"),
        "restore" => format!(
            "Restored {} removed item(s) to pack '{pack_id}'",
            restored_items.len()
        ),
        _ => format!("Updated pack '{pack_id}'"),
    };
    if !result.skipped_conflicts.is_empty() {
        summary.push_str(&format!(
            " — kept your edits to {} (not overwritten)",
            result.skipped_conflicts.join(", ")
        ));
    }

    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: action.to_string(),
        pack_id: pack_id.to_string(),
        changed_paths: result.changed_paths,
        skipped_conflicts: result.skipped_conflicts,
        restored_items,
        summary,
        ..Default::default()
    })
}

pub fn apply_pack_delete(orch: &Orchestrator, pack_id: &str) -> Result<PackMutationResult, String> {
    let result = uninstall_pack(&RealGitClient, &RealFileSystem, &orch.config_dir, pack_id)?;
    let mut summary = format!(
        "Uninstalled pack '{pack_id}' — removed {} item(s)",
        result.removed.len()
    );
    if !result.kept.is_empty() {
        summary.push_str(&format!(
            "; kept your edited copies of {}",
            result.kept.join(", ")
        ));
    }

    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: "uninstall".to_string(),
        pack_id: pack_id.to_string(),
        removed_paths: result.removed,
        kept_paths: result.kept,
        summary,
        ..Default::default()
    })
}

/// Take ONE item out of an installed pack, leaving the pack installed and still
/// updating around the gap.
///
/// The deletion itself runs through the same kind-specific function an ordinary
/// "delete this agent" performs, which is what records the removal against the
/// owning pack (`pack::note_removed_item`). Doing it any other way would give
/// the settings screen a second definition of what deleting a skill means — and
/// would, for an MCP server, be the difference between dropping a connector and
/// discarding the credential behind it.
pub fn apply_pack_item_removal(
    orch: &Orchestrator,
    pack_id: &str,
    kind: PackItemKind,
    item_id: &str,
) -> Result<PackMutationResult, String> {
    let lock = pack::lock::read_lock(&orch.config_dir, pack_id)
        .ok_or_else(|| format!("Pack `{pack_id}` is not installed"))?;
    if lock.is_removed(kind, item_id) {
        return Ok(PackMutationResult {
            action: "remove-item".to_string(),
            pack_id: pack_id.to_string(),
            removed_item: Some(PackItem {
                kind,
                id: item_id.to_string(),
                path: None,
            }),
            summary: format!(
                "{} `{item_id}` was already removed from pack '{pack_id}'",
                kind.as_str()
            ),
            ..Default::default()
        });
    }
    if !lock
        .items
        .iter()
        .any(|item| item.kind == kind && item.id == item_id)
    {
        return Err(format!(
            "Pack `{pack_id}` does not carry {} `{item_id}`",
            kind.as_str()
        ));
    }

    let config_dir = orch.config_dir.as_path();
    match kind {
        PackItemKind::Agent => crate::config::agents::delete_agent(config_dir, item_id, None)?,
        PackItemKind::Skill => crate::config::skills::delete_skill(config_dir, item_id, None)?,
        PackItemKind::Recipe => crate::config::recipes::delete_recipe(config_dir, item_id, None)?,
        PackItemKind::Response => {
            crate::config::responses::delete_response(config_dir, item_id, None)?;
        }
        PackItemKind::Workflow => {
            crate::config::workflows::delete_workflow(config_dir, item_id, None)?
        }
        PackItemKind::Mcp => {
            crate::config::mcp_servers::delete_workspace_mcp_server(config_dir, item_id)?
        }
    }

    // The delete above records the removal against whichever installed pack
    // claims the item. Re-reading the lock is how this reports the item as the
    // catalog will show it, and how a silently dropped record becomes an error
    // instead of a removal the next sync undoes.
    let removed = pack::lock::read_lock(config_dir, pack_id)
        .and_then(|lock| {
            lock.removed
                .into_iter()
                .find(|item| item.kind == kind && item.id == item_id)
        })
        .ok_or_else(|| {
            format!(
                "Removed {} `{item_id}`, but pack '{pack_id}' did not record it; the next sync \
                 would restore it",
                kind.as_str()
            )
        })?;

    orch.emit_pack_registry_change();
    Ok(PackMutationResult {
        action: "remove-item".to_string(),
        pack_id: pack_id.to_string(),
        removed_paths: removed.path.clone().into_iter().collect(),
        summary: format!(
            "Removed {} `{item_id}` from pack '{pack_id}' — the pack stays installed",
            kind.as_str()
        ),
        removed_item: Some(removed),
        ..Default::default()
    })
}

/// The `cairn://packs/{id}` patch arm: read the action off the payload and run
/// the same function the settings command runs.
pub(super) fn dispatch_pack_action(
    orch: &Orchestrator,
    payload: &serde_json::Value,
    pack_id: &str,
) -> Result<PackMutationResult, String> {
    let action = super::payload_trimmed_non_empty_str(payload, "action", &[])
        .ok_or("payload.action is required (install | update | restore)")?;
    apply_pack_action(orch, action, pack_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::resources::packs::{pack_catalog, PackItemState, PackState};
    use crate::services::testing::TestServicesBuilder;
    use crate::services::{EventEmitter, GitClient};
    use crate::storage::SearchIndex;
    use crate::workspace::bundle::sync_workspace_packs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    /// A capturing emitter the test can keep a handle on after handing it to
    /// the orchestrator.
    #[derive(Clone, Default)]
    struct RecordingEmitter {
        events: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
    }

    impl EventEmitter for RecordingEmitter {
        fn emit(&self, event: &str, payload: serde_json::Value) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push((event.to_string(), payload));
            Ok(())
        }

        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.emit(event, serde_json::Value::Null)
        }
    }

    struct Workspace {
        orch: Orchestrator,
        emitter: RecordingEmitter,
        home: PathBuf,
        resources: PathBuf,
        _temp: tempfile::TempDir,
    }

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn source_pack(resources: &Path, id: &str) -> PathBuf {
        resources.join("packs").join(id)
    }

    /// A `core` pack carrying one of every file-backed kind, plus an optional
    /// `matlab` pack whose only content is a connector — between them every
    /// `PackItemKind` is represented.
    fn write_resources(resources: &Path) {
        write(
            &source_pack(resources, "core").join("cairn-pack.yaml"),
            "id: core\nname: Core\nversion: 1.0.0\ndefault: true\n",
        );
        let core = source_pack(resources, "core");
        write(&core.join("agents/explore.md"), "bundle agent\n");
        write(&core.join("recipes/default.yaml"), "name: default\n");
        write(&core.join("responses/conveyor.md"), "response\n");
        write(
            &core.join("skills/example/SKILL.md"),
            "---\nname: Example\ndescription: Example skill\n---\n",
        );
        write(&core.join("workflows/flow/workflow.yaml"), "name: Flow\n");

        write(
            &source_pack(resources, "matlab").join("cairn-pack.yaml"),
            "id: matlab\nname: MATLAB\nversion: 1.0.0\ndefault: false\n",
        );
        write(
            &source_pack(resources, "matlab").join("mcp.yaml"),
            "mcpServers:\n  matlab:\n    type: stdio\n    command: ${MATLAB_BIN}\n",
        );
    }

    async fn workspace() -> Workspace {
        crate::config::secrets::mock_keychain::install();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_resources(&resources);
        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &home, "1.0.0").unwrap();

        let db = crate::storage::migrated_test_db("pack-mutations.db").await;
        let index = SearchIndex::open_or_create(temp.path().join("index")).unwrap();
        let db_state = Arc::new(DbState::new(Arc::new(db), Arc::new(index)));
        let emitter = RecordingEmitter::default();
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_emitter(emitter.clone())
                .build(),
        );
        let orch = Orchestrator::builder(db_state, services, home.clone()).build();
        Workspace {
            orch,
            emitter,
            home,
            resources,
            _temp: temp,
        }
    }

    fn pack_events(ws: &Workspace) -> usize {
        ws.emitter
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, payload)| name == "config-changed" && payload["entity_type"] == "pack")
            .count()
    }

    fn item_state(ws: &Workspace, pack_id: &str, kind: &str, id: &str) -> Option<PackItemState> {
        pack_catalog(&ws.home)
            .find(pack_id)?
            .items
            .iter()
            .find(|item| item.kind == kind && item.id == id)
            .map(|item| item.state)
    }

    /// Removing one item is a first-class operation for every kind a pack can
    /// carry — not just the file-backed ones, and never at the cost of the pack
    /// around it. The MCP arm is the one that would silently regress: its
    /// deletion path is the only one that also touches a settings file.
    #[tokio::test]
    async fn every_item_kind_can_be_removed_and_restored_without_uninstalling_its_pack() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();

        let cases: [(&str, PackItemKind, &str, &str); 6] = [
            ("core", PackItemKind::Agent, "explore", "agents/explore.md"),
            (
                "core",
                PackItemKind::Recipe,
                "default",
                "recipes/default.yaml",
            ),
            (
                "core",
                PackItemKind::Response,
                "conveyor",
                "responses/conveyor.md",
            ),
            ("core", PackItemKind::Skill, "example", "skills/example"),
            ("core", PackItemKind::Workflow, "flow", "workflows/flow"),
            ("matlab", PackItemKind::Mcp, "matlab", ""),
        ];

        for (pack_id, kind, id, path) in cases {
            let result = apply_pack_item_removal(&ws.orch, pack_id, kind, id).unwrap();
            assert_eq!(result.action, "remove-item");
            assert_eq!(result.removed_item.as_ref().unwrap().kind, kind);
            if !path.is_empty() {
                assert!(
                    !ws.home.join(path).exists(),
                    "{path} should be gone after removing {kind:?} `{id}`"
                );
            }
            assert_eq!(
                item_state(&ws, pack_id, kind.as_str(), id),
                Some(PackItemState::RemovedByUser),
                "the catalog must report {kind:?} `{id}` as removed by the user"
            );
            assert!(
                pack::lock::read_lock(&ws.home, pack_id).is_some(),
                "removing one item must leave pack `{pack_id}` installed"
            );
        }

        assert!(
            !crate::config::pack::mcp::load_pack_mcp_servers(&ws.home).contains_key("matlab"),
            "a removed connector stops being offered"
        );

        // A restore per pack brings every one of them back, materialized.
        for pack_id in ["core", "matlab"] {
            let restored = apply_pack_action(&ws.orch, "restore", pack_id).unwrap();
            assert!(!restored.restored_items.is_empty());
        }
        for path in [
            "agents/explore.md",
            "recipes/default.yaml",
            "responses/conveyor.md",
            "skills/example/SKILL.md",
            "workflows/flow/workflow.yaml",
        ] {
            assert!(
                ws.home.join(path).exists(),
                "restore must bring back {path}"
            );
        }
        assert!(crate::config::pack::mcp::load_pack_mcp_servers(&ws.home).contains_key("matlab"));
        for (pack_id, kind, id, _) in cases {
            assert_eq!(
                item_state(&ws, pack_id, kind.as_str(), id),
                Some(PackItemState::PackOwned)
            );
        }
    }

    /// An update must name the files it refused to overwrite. Flattened into
    /// prose those names can only be re-parsed; the settings screen reports them
    /// from the structured field.
    #[tokio::test]
    async fn an_update_reports_the_edits_it_kept_and_the_catalog_marks_them() {
        let ws = workspace().await;
        write(&ws.home.join("agents/explore.md"), "mine now\n");
        RealGitClient.add_all(&ws.home).unwrap();
        RealGitClient.commit(&ws.home, "My agent tweak").unwrap();

        assert_eq!(
            item_state(&ws, "core", "agent", "explore"),
            Some(PackItemState::EditedByUser)
        );

        write(
            &source_pack(&ws.resources, "core").join("agents/explore.md"),
            "shipped v2\n",
        );
        let result = apply_pack_action(&ws.orch, "update", "core").unwrap();

        assert_eq!(
            result.skipped_conflicts,
            vec!["agents/explore.md".to_string()]
        );
        assert_eq!(
            std::fs::read_to_string(ws.home.join("agents/explore.md")).unwrap(),
            "mine now\n",
            "an update must not overwrite what the user made theirs"
        );
    }

    /// An uninstall's two lists are different promises: what it deleted, and
    /// what it deliberately left behind.
    #[tokio::test]
    async fn an_uninstall_separates_what_it_removed_from_what_it_kept() {
        let ws = workspace().await;
        write(&ws.home.join("agents/explore.md"), "mine now\n");
        RealGitClient.add_all(&ws.home).unwrap();
        RealGitClient.commit(&ws.home, "My agent tweak").unwrap();

        let result = apply_pack_delete(&ws.orch, "core").unwrap();
        assert_eq!(result.action, "uninstall");
        assert_eq!(result.kept_paths, vec!["agents/explore.md".to_string()]);
        assert!(result
            .removed_paths
            .contains(&"recipes/default.yaml".to_string()));
        assert!(ws.home.join("agents/explore.md").exists());
        assert!(!ws.home.join("recipes/default.yaml").exists());

        let catalog = pack_catalog(&ws.home);
        let core = catalog.find("core").unwrap();
        assert_eq!(core.state, PackState::Available);
        assert!(core.uninstalled_by_user);
        assert!(
            core.installs_by_default,
            "an uninstall outranks the default set"
        );
    }

    /// The event is the whole point of the shared entry: it must fire exactly
    /// once per successful mutation, and never for one that failed — an event
    /// for a workspace that did not move is a refetch that can only confuse.
    #[tokio::test]
    async fn a_successful_mutation_emits_one_pack_change_and_a_failure_emits_none() {
        let ws = workspace().await;

        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        assert_eq!(pack_events(&ws), 1);

        assert!(
            apply_pack_action(&ws.orch, "install", "matlab").is_err(),
            "installing an installed pack is an error"
        );
        assert!(apply_pack_action(&ws.orch, "install", "nonexistent").is_err());
        assert!(apply_pack_delete(&ws.orch, "nonexistent").is_err());
        assert!(
            apply_pack_item_removal(&ws.orch, "core", PackItemKind::Agent, "nonexistent").is_err()
        );
        assert_eq!(pack_events(&ws), 1, "a failed mutation emits nothing");

        apply_pack_delete(&ws.orch, "matlab").unwrap();
        assert_eq!(pack_events(&ws), 2);
    }

    /// A restore discards the removal records BEFORE the sync that materializes
    /// the items runs. If that sync fails and the records stay discarded, the
    /// items are absent, the catalog no longer reports them as removed, and the
    /// only trace of a decision the user made is gone — with nothing on screen
    /// saying so. The failure must leave the workspace exactly as it was.
    #[tokio::test]
    async fn a_restore_that_cannot_materialize_leaves_the_removal_recorded() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        apply_pack_item_removal(&ws.orch, "matlab", PackItemKind::Mcp, "matlab").unwrap();
        let events_before = pack_events(&ws);

        // Force the materializing sync to fail after the restore has run: every
        // sync asserts the workspace's managed content directories exist, and a
        // plain file cannot be created as a directory.
        std::fs::remove_dir_all(ws.home.join("skills")).unwrap();
        std::fs::write(ws.home.join("skills"), "not a directory\n").unwrap();

        apply_pack_action(&ws.orch, "restore", "matlab")
            .expect_err("the sync cannot materialize into a file");

        assert_eq!(
            pack_events(&ws),
            events_before,
            "a failed restore emits nothing"
        );
        assert!(
            pack::lock::read_lock(&ws.home, "matlab")
                .expect("the pack is still installed")
                .is_removed(PackItemKind::Mcp, "matlab"),
            "a restore that could not materialize must leave the removal recorded, so the user \
             can try again"
        );
    }

    /// The same window, reached the way a real workspace reaches it: an
    /// installed pack whose shipped source is no longer on this machine. The
    /// catalog represents that state, so Restore is offered for it.
    #[tokio::test]
    async fn a_restore_with_no_shipped_source_is_refused_before_it_records_anything() {
        let ws = workspace().await;
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        apply_pack_item_removal(&ws.orch, "matlab", PackItemKind::Mcp, "matlab").unwrap();
        let events_before = pack_events(&ws);

        std::fs::remove_dir_all(source_pack(&ws.resources, "matlab")).unwrap();

        apply_pack_action(&ws.orch, "restore", "matlab")
            .expect_err("nothing ships this pack any more");

        assert_eq!(pack_events(&ws), events_before);
        assert!(pack::lock::read_lock(&ws.home, "matlab")
            .expect("the pack is still installed")
            .is_removed(PackItemKind::Mcp, "matlab"));
    }

    /// The catalog is the one model both surfaces read, so it has to carry
    /// every state a picker distinguishes on.
    #[tokio::test]
    async fn the_catalog_reports_installed_available_default_source_and_readiness() {
        let ws = workspace().await;
        let catalog = pack_catalog(&ws.home);
        assert!(catalog.source_recorded);

        let core = catalog.find("core").unwrap();
        assert_eq!(core.state, PackState::Installed);
        assert!(core.installs_by_default);
        assert!(!core.update_available);
        assert_eq!(core.installed_version.as_deref(), Some("1.0.0"));
        assert_eq!(core.shipped_version.as_deref(), Some("1.0.0"));
        assert_eq!(core.source.kind, "bundled");
        assert_eq!(
            core.counts.iter().map(|c| c.count).sum::<usize>(),
            5,
            "one of every file-backed kind"
        );

        let matlab = catalog.find("matlab").unwrap();
        assert_eq!(matlab.state, PackState::Available);
        assert!(!matlab.installs_by_default);
        assert!(matlab.installed_version.is_none());

        // Installing the connector surfaces the reason it is inert: its command
        // references a `${VAR}` with no value anywhere.
        apply_pack_action(&ws.orch, "install", "matlab").unwrap();
        let installed = pack_catalog(&ws.home);
        let connector = installed
            .find("matlab")
            .unwrap()
            .items
            .iter()
            .find(|item| item.kind == "mcp")
            .expect("the connector is an item");
        assert!(
            matches!(
                connector.not_ready.as_ref(),
                Some(crate::config::mcp_servers::NotReady::MissingVars { vars })
                    if vars == &["MATLAB_BIN".to_string()]
            ),
            "got {:?}",
            connector.not_ready
        );

        // A shipped change is what "update available" means.
        write(
            &source_pack(&ws.resources, "core").join("agents/explore.md"),
            "shipped v2\n",
        );
        assert!(
            pack_catalog(&ws.home)
                .find("core")
                .unwrap()
                .update_available
        );
    }
}
