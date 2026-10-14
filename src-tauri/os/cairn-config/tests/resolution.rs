//! The public boundary: actual workspace/project files become a launch selection.
use cairn_config::agents::get_agent;
use cairn_config::models::{Model, ModelSelection};
use cairn_config::presets::{
    load_effective_presets, resolve_agent_snapshot, resolve_selection_with_provenance,
    LaunchSelectionOverride, ResolutionSource,
};

const WORKSPACE: &str = r#"# Workspace-owned provider enablement
 enabledProviders: [claude, codex]
 tiers: [sm, md]
 tierDefaults: {sm: claude, md: claude}
 backends:
   claude:
     sm: {model: claude-haiku-4-5}
     md: {model: claude-sonnet-4-6}
   codex:
     sm: {model: gpt-5.4-mini}
     md: {model: gpt-5.5}
"#;
const PROJECT: &str = r#"# A legacy project default with a per-tier refinement
activeBackend: codex
tierDefaults: {sm: claude}
backends:
  codex:
    md: {model: gpt-6-astra}
futureSetting: {keep: exactly}
"#;

fn fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let project = root.path().join("project");
    std::fs::create_dir_all(workspace.join("agents")).unwrap();
    std::fs::create_dir_all(project.join(".cairn")).unwrap();
    std::fs::write(workspace.join("settings.yaml"), WORKSPACE).unwrap();
    std::fs::write(project.join(".cairn/config.yaml"), PROJECT).unwrap();
    std::fs::write(workspace.join("agents/worker.md"), "---\nname: Worker\ndescription: Configuration witness\ntier: md\ntools: []\n---\nDo the work.\n").unwrap();
    root
}

#[test]
fn file_layers_resolve_with_provenance_and_explicit_pins_without_rewriting_files() {
    let root = fixture();
    let workspace = root.path().join("workspace");
    let project = root.path().join("project");
    let config = load_effective_presets(&workspace, Some(&project));
    assert_eq!(config.tier_default("sm"), Some("claude"));
    assert_eq!(config.tier_default("md"), Some("codex"));
    // The project overrides one preset, not the whole backend's other tiers.
    assert_eq!(
        config.backends["codex"]["sm"].model.as_str(),
        "gpt-5.4-mini"
    );
    let resolved = resolve_selection_with_provenance(Some("md"), None, None, &config).unwrap();
    assert_eq!(resolved.selection.backend, "codex");
    assert_eq!(resolved.selection.model.as_str(), "gpt-6-astra");
    assert_eq!(resolved.source, ResolutionSource::TierDefaultBackend);

    let agent = get_agent(&workspace, "worker", Some(&project))
        .unwrap()
        .unwrap();
    let snapshot = resolve_agent_snapshot(&agent, None, &config).unwrap();
    assert_eq!(snapshot.selection, Some(resolved.selection));
    let pin = ModelSelection {
        backend: "codex".into(),
        model: Model::new("operator-pinned-model"),
    };
    let pinned = resolve_agent_snapshot(
        &agent,
        Some(&LaunchSelectionOverride::Concrete(pin.clone())),
        &config,
    )
    .unwrap();
    assert_eq!(pinned.selection, Some(pin));
    assert_eq!(
        std::fs::read_to_string(workspace.join("settings.yaml")).unwrap(),
        WORKSPACE
    );
    assert_eq!(
        std::fs::read_to_string(project.join(".cairn/config.yaml")).unwrap(),
        PROJECT
    );
}

#[test]
fn a_project_preset_and_explicit_pin_cannot_reenable_an_uninstalled_provider() {
    let root = fixture();
    let workspace = root.path().join("workspace");
    let project = root.path().join("project");
    std::fs::write(
        workspace.join("settings.yaml"),
        WORKSPACE.replace("[claude, codex]", "[claude]"),
    )
    .unwrap();
    let config = load_effective_presets(&workspace, Some(&project));
    assert!(!config.provider_enabled("codex"));
    assert!(
        config.backends.contains_key("codex"),
        "authored presets survive uninstall"
    );
    let agent = get_agent(&workspace, "worker", Some(&project))
        .unwrap()
        .unwrap();
    let pin = LaunchSelectionOverride::Concrete(ModelSelection {
        backend: "codex".into(),
        model: Model::new("gpt-6-astra"),
    });
    let error = resolve_agent_snapshot(&agent, Some(&pin), &config).unwrap_err();
    assert!(error.contains("not installed"), "{error}");
}
