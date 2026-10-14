use crate as pack;
use std::collections::HashSet;
use std::path::Path;

/// Restore the shipped contents of installed packs and every default pack.
/// Reinstallation clears uninstall markers and preserves files owned by other
/// packs and user-local configuration outside the restored pack items.
pub fn mirror_bundle_resources(resource_dir: &Path, config_dir: &Path) -> Result<(), String> {
    mirror_bundle_resources_with(
        &cairn_platform::services::RealGitClient,
        &cairn_platform::services::RealFileSystem,
        resource_dir,
        config_dir,
    )
}

/// The packs a "restore defaults" re-asserts: everything currently installed,
/// plus every pack that ships as a default.
///
/// The default set is included unconditionally, which is what the command's name
/// promises. Clicking it is a second explicit decision that supersedes an
/// earlier uninstall of a default pack — and going through the install path (see
/// below) is what clears that pack's uninstall marker, so the workspace never
/// ends up holding a pack's files while the catalog reports it uninstalled.
///
/// Within that scope the restore is total: edits, pack uninstalls, and item
/// removals alike are re-asserted to what the app ships.
fn restorable_packs(resource_dir: &Path, config_dir: &Path) -> Vec<pack::PackManifest> {
    let installed: HashSet<String> = pack::installed_packs(config_dir)
        .into_iter()
        .map(|lock| lock.id)
        .collect();
    pack::discover_available_packs(resource_dir)
        .into_iter()
        .filter(|manifest| manifest.default || installed.contains(&manifest.id))
        .collect()
}

pub fn mirror_bundle_resources_with(
    _git: &dyn cairn_platform::services::GitClient,
    fs: &dyn cairn_platform::services::FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
) -> Result<(), String> {
    fs.create_dir_all(config_dir)?;
    let packs = restorable_packs(resource_dir, config_dir);

    for manifest in &packs {
        crate::install::restore_one_bundled_pack(fs, resource_dir, config_dir, &manifest.id)?;
    }
    Ok(())
}

/// IDs restored by mirroring the installed packs' `dir_name` subtree.
pub fn bundled_resource_ids(
    resource_dir: &Path,
    config_dir: &Path,
    dir_name: &str,
) -> Result<Vec<String>, String> {
    let kind = match dir_name {
        "agents" => pack::PackItemKind::Agent,
        "recipes" => pack::PackItemKind::Recipe,
        "responses" => pack::PackItemKind::Response,
        "skills" => pack::PackItemKind::Skill,
        "workflows" => pack::PackItemKind::Workflow,
        other => return Err(format!("`{other}` is not a pack content directory")),
    };

    let mut ids: Vec<String> = restorable_packs(resource_dir, config_dir)
        .iter()
        .flat_map(|manifest| manifest.items())
        .filter(|item| item.kind == kind)
        .map(|item| item.id)
        .collect();
    ids.sort();
    ids.dedup();
    Ok(ids)
}

/// Result of restoring bundled defaults
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreResult {
    pub recipes_restored: Vec<String>,
    pub agents_restored: Vec<String>,
    pub skills_restored: Vec<String>,
}
