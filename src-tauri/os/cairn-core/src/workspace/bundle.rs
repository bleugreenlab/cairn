pub use cairn_packs::install::*;

#[cfg(test)]
use crate::config::{
    mcp_servers::McpServerConfigExt,
    pack::{self, lock as pack_lock, PackItemKind},
};
use std::path::Path;

/// Preserve desktop-automation opt-in when converting fleet selections. An
/// offered pack is not installed by the ordinary startup sync.
pub(crate) fn migrate_desktop_automation(
    git: &dyn crate::services::GitClient,
    fs: &dyn crate::services::FileSystem,
    config_dir: &Path,
) -> Result<Option<Vec<String>>, String> {
    use crate::config::{pack, settings};
    use cairn_config::fleet::{MachineServiceKey, AXON_MACHINE_SERVICE};

    let notes = settings::migrate_desktop_automation(config_dir)?;
    let key: MachineServiceKey = AXON_MACHINE_SERVICE.parse()?;
    if !settings::load_fleet(config_dir)
        .machine_services
        .machines_for(&key)
        .is_empty()
        && pack::lock::read_lock(config_dir, "axon").is_none()
    {
        // Also repairs a selection persisted by an earlier conversion, or by
        // a conversion whose pack install failed. Never resync an existing pack.
        let source = pack::source_dir(config_dir)
            .ok_or("Cannot migrate Axon selection: no recorded app resource directory")?;
        sync_one_pack(git, fs, &source, config_dir, "axon")?;
    }
    Ok(notes)
}
#[cfg(test)]
const DEFAULT_BRANCH: &str = "main";
#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{FileSystem, GitClient, RealFileSystem, RealGitClient};
    use cairn_config::library::{LibraryKind, LibraryRoots, Origin};
    const LEGACY_BUNDLE_SYNC_MARKER: &str = ".bundle-sync";
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    /// A shipped pack's source root inside a fake resource dir.
    fn source_pack(resources: &Path, id: &str) -> PathBuf {
        resources.join("packs").join(id)
    }

    fn write_manifest(resources: &Path, id: &str, default: bool) {
        write(
            &source_pack(resources, id).join("cairn-pack.yaml"),
            &format!("id: {id}\nname: {id}\nversion: 1.0.0\ndefault: {default}\n"),
        );
    }

    fn agent_body(body: &str) -> String {
        format!("---\nname: Explore\ndescription: d\ntools: Read\n---\n\n{body}\n")
    }

    /// A complete default `core` pack: one of every library kind.
    fn write_complete_resources(resources: &Path) {
        write_manifest(resources, "core", true);
        let root = source_pack(resources, "core");
        write(&root.join("agents/explore.md"), &agent_body("bundle agent"));
        write(&root.join("recipes/default.yaml"), "name: default\n");
        write(&root.join("responses/conveyor.md"), "response\n");
        write(&root.join("routes/followed.yaml"), "name: followed\n");
        write(
            &root.join("skills/example/SKILL.md"),
            "---\nname: example\ndescription: Example skill\n---\n",
        );
        write(
            &root.join("workflows/example/workflow.yaml"),
            "name: Example\n",
        );
    }

    /// A non-default pack carrying one skill and one MCP server, the `matlab`
    /// archetype in miniature.
    fn write_optional_pack(resources: &Path, id: &str) {
        write_manifest(resources, id, false);
        let root = source_pack(resources, id);
        write(
            &root.join(format!("skills/{id}/SKILL.md")),
            &format!("---\nname: {id}\ndescription: Optional skill\n---\n"),
        );
        write(
            &root.join("mcp.yaml"),
            &format!("mcpServers:\n  {id}:\n    type: stdio\n    command: ${{{id}_BIN}}\n"),
        );
    }

    /// Every file in the workspace except git internals, so a sync can be
    /// asserted to have written nothing.
    fn snapshot_tree(root: &Path) -> BTreeMap<String, String> {
        fn walk(root: &Path, dir: &Path, into: &mut BTreeMap<String, String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.file_name().and_then(|n| n.to_str()) == Some(".git") {
                    continue;
                }
                if path.is_dir() {
                    walk(root, &path, into);
                } else {
                    let rel = path
                        .strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .to_string();
                    let body = std::fs::read_to_string(&path).unwrap_or_default();
                    into.insert(rel, body);
                }
            }
        }
        let mut map = BTreeMap::new();
        walk(root, root, &mut map);
        map
    }

    /// A workspace exactly as the pre-pack bundle sync left one: every shipped
    /// file materialized flat, committed under a legacy bundle subject, and a
    /// global `.bundle-sync` marker. No `packs/` directory anywhere.
    fn seed_legacy_workspace(repo: &Path, resources: &Path, pack_ids: &[&str]) {
        std::fs::create_dir_all(repo).unwrap();
        for id in pack_ids {
            let root = source_pack(resources, id);
            for dir_name in pack::CONTENT_DIRS {
                let source = root.join(dir_name);
                if source.exists() {
                    RealFileSystem
                        .copy_dir_recursive(&source, &repo.join(dir_name))
                        .unwrap();
                }
            }
        }
        std::fs::write(repo.join(LEGACY_BUNDLE_SYNC_MARKER), "legacy-hash\n").unwrap();
        let git = RealGitClient;
        git.init_repo(repo, DEFAULT_BRANCH).unwrap();
        git.add_all(repo).unwrap();
        git.commit(repo, "Initialize Cairn workspace config")
            .unwrap();
    }

    fn shipped_resources() -> &'static Path {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("cairn-core manifest is nested under src-tauri")
    }

    fn ships_service(repo: &Path, pack_id: &str, service: &str) -> bool {
        pack_lock::read_lock(repo, pack_id)
            .is_some_and(|lock| lock.item(PackItemKind::Service, service).is_some())
    }

    #[test]
    fn desktop_automation_upgrade_installs_axon_only_for_selected_machines() {
        use cairn_config::fleet::MachineServiceKey;

        for enabled in [true, false] {
            let temp = TempDir::new().unwrap();
            let repo = temp.path().join("home");
            write(
                &repo.join("settings.yaml"),
                &format!("buildSlots:\n  desktopAutomation:\n    enabled: {enabled}\n    machines:\n      local: {{}}\n"),
            );
            sync_workspace_packs(
                &RealGitClient,
                &RealFileSystem,
                shipped_resources(),
                &repo,
                "1.0.0",
            )
            .unwrap();
            assert!(pack_lock::read_lock(&repo, "axon").is_none());
            migrate_desktop_automation(&RealGitClient, &RealFileSystem, &repo).unwrap();

            let key: MachineServiceKey = "axon/axon".parse().unwrap();
            let fleet = crate::config::settings::load_fleet(&repo);
            let selection = fleet.machine_services.selection(&key, "local");
            assert_eq!(selection.is_some(), enabled);
            assert!(selection.is_none_or(|selected| selected.override_command().is_none()));
            assert_eq!(pack_lock::read_lock(&repo, "axon").is_some(), enabled);
            assert_eq!(ships_service(&repo, "axon", "axon"), enabled);
            assert!(
                !repo.join("services/axon").exists(),
                "a service is never mirrored into the workspace"
            );
            assert_eq!(repo.join("packs/axon/mcp.yaml").exists(), enabled);
            let before = snapshot_tree(&repo);
            assert!(
                migrate_desktop_automation(&RealGitClient, &RealFileSystem, &repo)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                snapshot_tree(&repo),
                before,
                "repeat migration writes nothing"
            );
        }
    }

    #[test]
    fn an_already_migrated_axon_selection_installs_the_offered_pack() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        write(
            &repo.join("settings.yaml"),
            "buildSlots:\n  machineServices:\n    axon/axon:\n      machines:\n        local: {}\n",
        );
        sync_workspace_packs(
            &RealGitClient,
            &RealFileSystem,
            shipped_resources(),
            &repo,
            "1.0.0",
        )
        .unwrap();
        let settings = std::fs::read(repo.join("settings.yaml")).unwrap();
        migrate_desktop_automation(&RealGitClient, &RealFileSystem, &repo).unwrap();
        assert!(pack_lock::read_lock(&repo, "axon").is_some());
        assert!(ships_service(&repo, "axon", "axon"));
        assert_eq!(std::fs::read(repo.join("settings.yaml")).unwrap(), settings);
    }

    #[test]
    fn an_installed_axon_pack_is_untouched_by_the_opt_in_migration() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        sync_one_pack(
            &RealGitClient,
            &RealFileSystem,
            shipped_resources(),
            &repo,
            "axon",
        )
        .unwrap();
        write(
            &repo.join("settings.yaml"),
            "buildSlots:\n  machineServices:\n    axon/axon:\n      machines:\n        local: {}\n",
        );
        let before = snapshot_tree(&repo);
        migrate_desktop_automation(&RealGitClient, &RealFileSystem, &repo).unwrap();
        assert_eq!(snapshot_tree(&repo), before);
        sync_workspace_packs(
            &RealGitClient,
            &RealFileSystem,
            shipped_resources(),
            &repo,
            "1.0.0",
        )
        .unwrap();
        assert!(
            pack_lock::read_lock(&repo, "axon").is_some(),
            "offered does not mean uninstalled"
        );
    }

    /// The acceptance path, exercised against the packs this repository actually
    /// ships rather than a fixture: a fresh workspace installs `core` only, read
    /// in place under `packs/core/`, while `axon`, `matlab`, and `linear` wait
    /// in the catalog. Fixtures can drift from the shipped tree; this cannot.
    #[test]
    fn the_shipped_packs_install_the_default_set_and_offer_the_rest() {
        crate::config::secrets::mock_keychain::install();
        let src_tauri = shipped_resources();
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let git = RealGitClient;
        let fs = RealFileSystem;

        sync_workspace_packs(&git, &fs, src_tauri, &repo, "1.0.0").unwrap();

        for path in [
            "agents/build.md",
            "recipes/build.yaml",
            "responses/conveyor.md",
            "routes/ambient-channel.yaml",
            "routes/github-mention.yaml",
            "skills/browser/SKILL.md",
            "workflows/fan-out/workflow.yaml",
        ] {
            assert!(
                repo.join("packs/core").join(path).exists(),
                "core must ship {path}"
            );
            assert!(!repo.join(path).exists(), "{path} is not copied out");
        }
        let core = pack_lock::read_lock(&repo, "core").expect("core lock");
        for id in ["ambient-channel", "github-mention"] {
            assert!(
                core.item(PackItemKind::Route, id).is_some(),
                "core must record shipped route `{id}` as a pack item"
            );
        }
        assert!(
            !repo
                .join("packs/core/routes/followed-thread-stream.yaml")
                .exists(),
            "the retired route is not installed"
        );
        let roots = LibraryRoots::workspace(&repo);
        assert_eq!(
            crate::config::agents::resolve_agent(&roots, "build")
                .unwrap()
                .origin,
            Origin::Pack { id: "core".into() }
        );

        // The optional packs are offered, not installed.
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
        assert!(pack_lock::read_lock(&repo, "linear").is_none());
        assert!(pack_lock::read_lock(&repo, "axon").is_none());
        let library = crate::library::LibraryContext::configured();
        let catalog = crate::resources::packs::read_packs(
            &repo,
            &library,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
            None,
        );
        assert!(catalog.contains("`matlab`"), "{catalog}");
        assert!(catalog.contains("`linear`"), "{catalog}");
        let installed: std::collections::BTreeSet<_> = pack_lock::installed_packs(&repo)
            .into_iter()
            .map(|pack| pack.id)
            .collect();
        assert_eq!(
            installed,
            std::collections::BTreeSet::from(["core".to_string()]),
            "only core is installed on a fresh workspace"
        );

        // Installing matlab lands its skill AND its MCP definition.
        sync_one_pack(&git, &fs, src_tauri, &repo, "matlab").unwrap();
        assert!(repo.join("packs/matlab/skills/matlab/SKILL.md").exists());
        assert!(repo
            .join("packs/matlab/skills/matlab/scripts/inspect-mat.py")
            .exists());
        assert!(repo.join("packs/matlab/mcp.yaml").exists());

        // Installing linear lands nothing but a connector and its OAuth config.
        sync_one_pack(&git, &fs, src_tauri, &repo, "linear").unwrap();
        let layer = pack::mcp::load_pack_mcp_servers(&repo);
        let linear = &layer["linear"];
        assert_eq!(linear.pack_id, "linear");
        assert_eq!(linear.config.transport, "http");
        assert!(linear.config.oauth.is_some());

        let entries = crate::config::mcp_servers::workspace_mcp_entries(
            &repo,
            &crate::config::machine_services::MachineServiceSnapshot::empty(),
        );
        assert_eq!(entries["linear"].origin, "pack:linear");
        assert_eq!(entries["matlab"].origin, "pack:matlab");
        assert!(entries["linear"].not_ready.is_some());

        // Uninstalling gives the workspace back exactly what it had.
        uninstall_pack(&git, &fs, &repo, "matlab").unwrap();
        assert!(!repo.join("packs/matlab/skills").exists());
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
        assert!(!crate::config::mcp_servers::workspace_mcp_entries(
            &repo,
            &crate::config::machine_services::MachineServiceSnapshot::empty()
        )
        .contains_key("matlab"));
    }

    /// Every connector this repository ships must actually reach the catalog,
    /// and one that asks for nothing must be ready the moment it installs.
    #[test]
    fn every_shipped_connector_reaches_the_catalog_and_states_its_readiness() {
        use crate::config::mcp_servers::NotReady;

        crate::config::secrets::mock_keychain::install();
        let src_tauri = shipped_resources();
        let packs_dir = src_tauri.join(pack::PACKS_SOURCE_DIR);

        let mut shipped: Vec<String> = std::fs::read_dir(&packs_dir)
            .expect("read src-tauri/packs")
            .map(|entry| entry.expect("readable pack entry").path())
            .filter(|path| path.is_dir())
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        shipped.sort();

        let discovered: Vec<String> = pack::discover_available_packs(src_tauri)
            .into_iter()
            .map(|manifest| manifest.id)
            .collect();
        assert_eq!(
            discovered, shipped,
            "every shipped pack directory must resolve to a manifest whose id is its \
             directory name; a pack missing from this list failed to parse and was skipped"
        );

        let mut declared_by: BTreeMap<String, String> = BTreeMap::new();
        for id in &shipped {
            let mcp_file = packs_dir.join(id).join(pack::manifest::PACK_MCP_FILE);
            if !mcp_file.is_file() {
                continue;
            }
            let file = pack::mcp::parse_pack_mcp_file(&mcp_file)
                .unwrap_or_else(|e| panic!("pack `{id}` ships an unparseable mcp.yaml: {e}"));
            assert!(
                !file.servers.is_empty(),
                "pack `{id}` ships an mcp.yaml declaring no servers"
            );
            for (name, config) in &file.servers {
                for var in config.referenced_vars() {
                    let described = file
                        .setup
                        .get(&var)
                        .is_some_and(|entry| entry.label.is_some() || entry.description.is_some());
                    assert!(
                        described,
                        "pack `{id}` server `{name}` gates on ${{{var}}} with no `setup` entry \
                         describing it; the settings row would have nothing to ask for"
                    );
                }
            }
            for (name, config) in file.servers {
                if let Some(other) = declared_by.insert(name.clone(), id.clone()) {
                    panic!(
                        "packs `{other}` and `{id}` both declare MCP server `{name}`; the \
                         install path refuses that collision, so one of them could never \
                         be installed alongside the other"
                    );
                }
                if matches!(config.transport.as_str(), "http" | "sse") {
                    let url = config.url.as_deref().unwrap_or_default();
                    assert!(
                        url.starts_with("https://"),
                        "remote connector `{name}` must reach its server over https, got {url:?}"
                    );
                }
                let readiness = config.readiness(&name);
                if config.oauth.is_some() {
                    assert_eq!(
                        readiness,
                        Some(NotReady::NeedsAuth),
                        "OAuth connector `{name}` must sit needs-auth until the user authorizes"
                    );
                } else if config.referenced_vars().is_empty() {
                    assert_eq!(
                        readiness, None,
                        "connector `{name}` asks for no credential and no variable, so it \
                         must be usable the moment its pack installs"
                    );
                }
            }
        }
    }

    #[test]
    fn fresh_install_installs_only_default_packs() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");

        let result =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert!(result.updated);
        assert!(
            !repo.join(".git").exists(),
            "fresh pack sync does not initialize Git"
        );
        assert!(repo.join("packs/core/agents/explore.md").exists());
        assert!(repo.join("packs/core/responses/conveyor.md").exists());
        let core = pack_lock::read_lock(&repo, "core").expect("core lock");
        assert_eq!(core.source.kind, pack::PackSourceKind::Bundled);
        assert!(core.items.iter().any(|item| item.id == "explore"));
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
        assert!(!repo.join("packs/matlab").exists());
    }

    /// A shipped route takes the pack's next version in place; a route the
    /// user defines under the same name is a different file in front of it,
    /// untouched by the update and in force until they delete it.
    #[test]
    fn a_shipped_route_updates_in_place_under_the_users_own() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        let git = RealGitClient;
        let fs = RealFileSystem;
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        write(&repo.join("routes/followed.yaml"), "name: mine\n");

        write(
            &source_pack(&resources, "core").join("routes/followed.yaml"),
            "name: followed v2\n",
        );
        sync_workspace_packs(&git, &fs, &resources, &repo, "2.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("routes/followed.yaml")).unwrap(),
            "name: mine\n"
        );
        assert_eq!(
            std::fs::read_to_string(repo.join("packs/core/routes/followed.yaml")).unwrap(),
            "name: followed v2\n"
        );
    }

    #[test]
    fn installed_pack_writes_its_mcp_layer_and_records_the_server() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");
        write_manifest(&resources, "matlab", true);

        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();

        assert!(repo.join("packs/matlab/skills/matlab/SKILL.md").exists());
        assert!(repo.join("packs/matlab/mcp.yaml").exists());
        let lock = pack_lock::read_lock(&repo, "matlab").expect("matlab lock");
        assert!(lock
            .items
            .iter()
            .any(|item| item.kind == pack::PackItemKind::Mcp && item.id == "matlab"));
        let layer = pack::mcp::load_pack_mcp_servers(&repo);
        assert_eq!(layer["matlab"].pack_id, "matlab");
        assert_eq!(layer["matlab"].config.transport, "stdio");
    }

    /// A workspace from before packs holds flat copies committed by the old
    /// bundle sync. The first in-place sync adopts the packs, deletes the
    /// copies it can prove are theirs, keeps one the user edited, and commits
    /// the move once.
    #[test]
    fn a_pre_pack_workspace_migrates_its_copies_into_the_packs() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");
        seed_legacy_workspace(&repo, &resources, &["core", "matlab"]);
        // An edit made after the old sync's commit is the user's.
        write(&repo.join("agents/explore.md"), &agent_body("my edit"));

        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();

        assert!(!repo.join("recipes/default.yaml").exists());
        assert!(!repo.join("skills/matlab").exists());
        assert!(repo.join("packs/core/recipes/default.yaml").exists());
        assert!(
            pack_lock::read_lock(&repo, "matlab").is_some(),
            "a pack whose content the user already has is recorded as installed"
        );
        let roots = LibraryRoots::workspace(&repo);
        let explore = crate::config::agents::resolve_agent(&roots, "explore").unwrap();
        assert_eq!(explore.origin, Origin::Workspace, "the edited copy is kept");
        assert_eq!(explore.shadows, vec![Origin::Pack { id: "core".into() }]);
        assert!(!repo.join(LEGACY_BUNDLE_SYNC_MARKER).exists());
        let subject = RealGitClient
            .run(&repo, vec!["log".into(), "-1".into(), "--format=%s".into()])
            .unwrap()
            .stdout;
        assert_eq!(subject.trim(), pack::migrate_v3::MIGRATION_COMMIT);

        let after = snapshot_tree(&repo);
        let again =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert!(!again.updated, "{again:?}");
        assert_eq!(snapshot_tree(&repo), after);
    }

    /// An uninstall must survive the next startup, and a user's own definition
    /// of a name the pack shipped is a separate file the uninstall never touches.
    #[test]
    fn an_uninstall_holds_across_a_restart_and_keeps_your_definitions() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");
        let git = RealGitClient;
        let fs = RealFileSystem;

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        sync_one_pack(&git, &fs, &resources, &repo, "matlab").unwrap();
        write(
            &repo.join("skills/matlab/SKILL.md"),
            "---\nname: matlab\ndescription: mine\n---\n",
        );

        uninstall_pack(&git, &fs, &repo, "matlab").unwrap();
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(
            pack_lock::read_lock(&repo, "matlab").is_none(),
            "an uninstall must not be undone by the next sync"
        );
        assert!(!pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"));
        assert!(repo.join("skills/matlab/SKILL.md").exists());

        sync_one_pack(&git, &fs, &resources, &repo, "matlab").unwrap();
        assert!(pack_lock::read_lock(&repo, "matlab").is_some());
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(pack_lock::read_lock(&repo, "matlab").is_some());
    }

    /// Removing one item keeps the pack installed and updating around it: a
    /// removed library item stays staged but resolves for nothing, a removed
    /// MCP server leaves the layer, and restoring is one explicit action.
    #[test]
    fn a_removed_item_stays_removed_while_its_pack_keeps_updating() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");
        write_manifest(&resources, "matlab", true);
        let git = RealGitClient;
        let fs = RealFileSystem;

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        crate::config::mcp_servers::delete_workspace_mcp_server(&repo, "matlab").unwrap();
        assert!(!pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"));
        crate::config::skills::delete_skill(&LibraryRoots::workspace(&repo), "example", None)
            .unwrap();

        write(
            &source_pack(&resources, "core").join("agents/explore.md"),
            &agent_body("updated agent"),
        );
        sync_workspace_packs(&git, &fs, &resources, &repo, "2.0.0").unwrap();

        let roots = LibraryRoots::workspace(&repo);
        assert!(crate::config::agents::get_agent(&roots, "explore")
            .unwrap()
            .unwrap()
            .prompt
            .contains("updated agent"));
        assert!(!pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"));
        assert!(crate::config::skills::get_skill(&roots, "example")
            .unwrap()
            .is_none());
        assert!(
            repo.join("packs/core/skills/example/SKILL.md").exists(),
            "a removed item stays staged so restoring it is only the record"
        );
        assert!(crate::config::skills::get_skill(&roots, "matlab")
            .unwrap()
            .is_some());

        pack_lock::restore_removed_items(&fs, &repo, "core").unwrap();
        pack_lock::restore_removed_items(&fs, &repo, "matlab").unwrap();
        sync_one_pack(&git, &fs, &resources, &repo, "matlab").unwrap();
        let roots = LibraryRoots::workspace(&repo);
        assert!(crate::config::skills::get_skill(&roots, "example")
            .unwrap()
            .is_some());
        assert!(pack::mcp::load_pack_mcp_servers(&repo).contains_key("matlab"));
    }

    /// The same guarantee for a `default: true` pack, which the planner would
    /// otherwise reinstall wholesale on every launch.
    #[test]
    fn uninstalling_a_default_pack_also_holds() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        let git = RealGitClient;
        let fs = RealFileSystem;
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        uninstall_pack(&git, &fs, &repo, "core").unwrap();
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        assert!(pack_lock::read_lock(&repo, "core").is_none());
        assert!(!repo.join("packs/core/agents/explore.md").exists());
    }

    #[test]
    fn migration_leaves_a_pack_whose_content_was_deleted_available() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_optional_pack(&resources, "matlab");
        seed_legacy_workspace(&repo, &resources, &["core"]);
        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        assert!(pack_lock::read_lock(&repo, "core").is_some());
        assert!(pack_lock::read_lock(&repo, "matlab").is_none());
    }

    #[test]
    fn unchanged_pack_content_short_circuits() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        let again =
            sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0")
                .unwrap();
        assert_eq!(again, PackSyncResult::default());
    }

    /// A route deleted from its pack's source leaves a workspace that had it
    /// installed at the next sync: the staged tree is replaced whole, so the
    /// installed copy goes and the pack no longer records it.
    #[test]
    fn a_route_deleted_from_its_pack_is_removed_from_an_installed_workspace() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        let git = RealGitClient;
        let fs = RealFileSystem;
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        let installed = repo.join("packs/core/routes/followed.yaml");
        assert!(installed.exists());

        std::fs::remove_file(source_pack(&resources, "core").join("routes/followed.yaml")).unwrap();
        write(
            &source_pack(&resources, "core").join("cairn-pack.yaml"),
            "id: core\nname: core\nversion: 1.0.0\ndefault: true\nretired:\n  - routes/followed.yaml\n",
        );
        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();

        assert!(!installed.exists(), "the installed copy is removed");
        let core = pack_lock::read_lock(&repo, "core").expect("core lock");
        assert!(core.item(PackItemKind::Route, "followed").is_none());
        assert!(
            crate::config::routes::get_route(&LibraryRoots::workspace(&repo), "followed")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_retired_resource_is_no_longer_shipped() {
        // Retiring a pack resource means deleting it from the pack's source
        // tree; listing it under `retired:` alone would leave it shipping.
        let packs = pack::discover_available_packs(shipped_resources());
        assert!(
            !packs.is_empty(),
            "the repo ships packs under src-tauri/packs"
        );
        for manifest in packs {
            for rel_path in &manifest.retired {
                let shipped = manifest.root.join(rel_path);
                assert!(
                    !shipped.exists(),
                    "{rel_path} is retired by pack `{}` but is still shipped at {shipped:?}; \
                     delete it from the pack or drop it from `retired:`",
                    manifest.id
                );
            }
        }
    }

    #[test]
    fn provision_runtime_syncs_then_short_circuits_then_resyncs_on_change() {
        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        let cairn_home = temp.path().join("home");
        let harness = resource_dir.join("runtime/node_modules/@cairn/harness");
        write(
            &harness.join("package.json"),
            "{\"name\":\"@cairn/harness\"}",
        );
        write(&harness.join("src/index.ts"), "export const v = 1;\n");

        let fs = RealFileSystem;
        assert!(provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());
        let dest_harness = cairn_home.join("runtime/node_modules/@cairn/harness/src/index.ts");
        assert!(dest_harness.exists());
        assert!(cairn_home.join("runtime/.runtime-version").exists());
        assert!(!provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());
        std::fs::write(harness.join("src/index.ts"), "export const v = 2;\n").unwrap();
        assert!(provision_workflow_runtime(&fs, &resource_dir, &cairn_home).unwrap());
        assert_eq!(
            std::fs::read_to_string(&dest_harness).unwrap(),
            "export const v = 2;\n"
        );
    }

    #[test]
    fn provision_runtime_is_noop_without_a_bundled_runtime() {
        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        std::fs::create_dir_all(&resource_dir).unwrap();
        let cairn_home = temp.path().join("home");
        assert!(!provision_workflow_runtime(&RealFileSystem, &resource_dir, &cairn_home).unwrap());
        assert!(!cairn_home.join("runtime").exists());
    }

    /// A pack's machine service is inventoried by its lock and never copied
    /// into the workspace: it runs from the app bundle's declaration.
    #[test]
    fn a_packed_service_is_inventoried_and_never_mirrored() {
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_manifest(&resources, "kit", true);
        let root = source_pack(&resources, "kit");
        write(&root.join("services/probe/service.yaml"), "name: Probe\n");
        write(
            &root.join("mcp.yaml"),
            "mcpServers:\n  probe:\n    type: machine-service\n    service: probe\n",
        );
        sync_workspace_packs(&RealGitClient, &RealFileSystem, &resources, &repo, "1.0.0").unwrap();
        let lock = pack_lock::read_lock(&repo, "kit").unwrap();
        assert!(lock.item(PackItemKind::Service, "probe").is_some());
        assert!(lock.item(PackItemKind::Mcp, "probe").is_some());
        assert!(!repo.join("services/probe").exists());
        assert!(!repo.join("packs/kit/services").exists());
        let entry = &pack::mcp::load_pack_mcp_servers(&repo)["probe"];
        assert_eq!(entry.config.service.as_deref(), Some("probe"));
    }

    /// Shipped workflows load on a fresh workspace and take pack updates in
    /// place; a workflow the user defines under the same name is theirs and
    /// survives every later sync.
    #[test]
    fn a_packed_workflow_lists_updates_in_place_and_yields_to_yours() {
        fn write_workflow(resources: &Path, body: &str) {
            let root = source_pack(resources, "core").join("workflows/fan-out");
            write(
                &root.join("workflow.yaml"),
                "name: Fan Out\ndescription: Zero-authoring ad-hoc agent batch.\n",
            );
            write(&root.join("main.ts"), body);
        }
        let temp = TempDir::new().unwrap();
        let repo = temp.path().join("home");
        let resources = temp.path().join("resources");
        write_complete_resources(&resources);
        write_workflow(&resources, "// v1\n");
        let git = RealGitClient;
        let fs = RealFileSystem;

        sync_workspace_packs(&git, &fs, &resources, &repo, "1.0.0").unwrap();
        let roots = LibraryRoots::workspace(&repo);
        let listed = crate::config::workflows::list_workflow_rows(&roots).unwrap();
        assert!(listed
            .iter()
            .any(|row| row.name == "fan-out" && row.origin == Origin::Pack { id: "core".into() }));

        write_workflow(&resources, "// v2\n");
        sync_workspace_packs(&git, &fs, &resources, &repo, "2.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("packs/core/workflows/fan-out/main.ts")).unwrap(),
            "// v2\n"
        );

        write(
            &repo.join("workflows/fan-out/workflow.yaml"),
            "name: Mine\ndescription: d\n",
        );
        write(&repo.join("workflows/fan-out/main.ts"), "// mine\n");
        write_workflow(&resources, "// v3\n");
        sync_workspace_packs(&git, &fs, &resources, &repo, "3.0.0").unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("workflows/fan-out/main.ts")).unwrap(),
            "// mine\n"
        );
        let resolved =
            crate::config::workflows::resolve_workflow(&LibraryRoots::workspace(&repo), "fan-out")
                .unwrap();
        assert_eq!(resolved.origin, Origin::Workspace);
        let _ = LibraryKind::Workflow;
    }
}
