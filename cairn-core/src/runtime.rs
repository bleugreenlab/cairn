//! The runner's own copy of the Cairn JavaScript packages, and how it reaches
//! the machine that runs a batch.
//!
//! [`cairn_common::runtime`] owns WHERE these packages install and why placement
//! rather than a resolver hook is the mechanism. This module owns WHAT is
//! installed: the version-matched `@cairn/sdk` that ships with this build, read
//! from the runtime the app provisions into `<CAIRN_HOME>/runtime` (or, in a
//! development build, straight from the repository's `packages/`).
//!
//! One source serves every surface that runs Cairn-owned JavaScript -- workflows,
//! inline TypeScript and JavaScript, and the TypeScript REPL -- so those surfaces
//! cannot drift onto different copies of the SDK.

use std::path::{Path, PathBuf};

use cairn_common::executor_protocol::ResidentRuntimeAsset;

/// The npm scope every Cairn-owned package lives under.
pub const SCOPE: &str = "@cairn";

/// Locate one Cairn-owned runtime package on this machine.
///
/// A release build reads the tree the app provisioned into `<CAIRN_HOME>/runtime`
/// (see `workspace::bundle::provision_workflow_runtime`). A development build
/// falls back to the repository's own `packages/`, so a `cargo run` with no
/// staged runtime still has a version-matched SDK -- the same one the developer
/// is editing.
pub fn runtime_package(name: &str) -> Result<PathBuf, String> {
    if let Some(installed) = installed_runtime_package(&cairn_common::paths::cairn_home(), name) {
        return Ok(installed);
    }
    #[cfg(debug_assertions)]
    {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo = manifest
            .ancestors()
            .nth(4)
            .ok_or("cannot locate Cairn development repository root")?;
        let dev = repo.join("packages").join(name);
        if dev.join("package.json").is_file() {
            return Ok(dev);
        }
    }
    Err(format!(
        "Cairn runtime package {SCOPE}/{name} is not installed under CAIRN_HOME/runtime/node_modules"
    ))
}

fn installed_runtime_package(home: &Path, name: &str) -> Option<PathBuf> {
    let installed = home.join("runtime/node_modules/@cairn").join(name);
    installed
        .join("package.json")
        .is_file()
        .then_some(installed)
}

/// Read every file of `source` into runtime assets addressed under `destination`.
pub fn collect_package_assets(
    source: &Path,
    destination: &Path,
    out: &mut Vec<ResidentRuntimeAsset>,
) -> Result<(), String> {
    for entry in std::fs::read_dir(source).map_err(|e| format!("read {}: {e}", source.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let target = destination.join(entry.file_name());
        let ty = entry.file_type().map_err(|e| e.to_string())?;
        if ty.is_dir() {
            collect_package_assets(&entry.path(), &target, out)?;
        } else if ty.is_file() {
            out.push(ResidentRuntimeAsset {
                path: target.to_string_lossy().replace('\\', "/"),
                data: std::fs::read(entry.path()).map_err(|e| e.to_string())?,
            });
        }
    }
    Ok(())
}

/// The SDK as runtime assets addressed relative to a `node_modules` root, ready
/// to travel to whichever machine runs the batch.
pub fn sdk_packages() -> Result<Vec<ResidentRuntimeAsset>, String> {
    let mut assets = Vec::new();
    collect_package_assets(
        &runtime_package("sdk")?,
        Path::new(SCOPE).join("sdk").as_path(),
        &mut assets,
    )?;
    cairn_common::runtime::validate_runtime_assets(&assets)?;
    Ok(assets)
}

/// Install the SDK for a checkout this host runs against itself.
///
/// `Ok(None)` means the checkout has no Cairn-owned ancestor (a live checkout in
/// the user's own repository), which is a statement about the destination rather
/// than a failure to install: Cairn does not write packages into a repository it
/// does not own.
pub fn install_sdk_for_checkout(checkout: &Path) -> Result<Option<PathBuf>, String> {
    let Some(parent) = cairn_common::runtime::shared_runtime_parent(checkout) else {
        return Ok(None);
    };
    let assets = sdk_packages()?;
    let version = cairn_common::runtime::assets_version(&assets);
    cairn_common::runtime::install_shared_runtime(&parent, &assets, &version).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A release build has no source checkout to fall back to, so the provisioned
    /// runtime under CAIRN_HOME has to be found on its own.
    #[test]
    fn installed_runtime_resolves_without_a_source_checkout() {
        let temp = tempfile::tempdir().unwrap();
        let package = temp.path().join("runtime/node_modules/@cairn/harness");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(package.join("package.json"), "{}").unwrap();

        assert_eq!(
            installed_runtime_package(temp.path(), "harness"),
            Some(package)
        );
    }

    /// A directory without a `package.json` is not a package. Accepting one would
    /// install an empty tree and turn a missing runtime into an import error
    /// blamed on the user's code.
    #[test]
    fn a_directory_without_a_manifest_is_not_a_package() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("runtime/node_modules/@cairn/sdk")).unwrap();
        assert_eq!(installed_runtime_package(temp.path(), "sdk"), None);
    }

    /// Assets are addressed relative to the `node_modules` they install into, so
    /// the paths the executor writes are exactly what bun's upward walk expects
    /// to find.
    #[test]
    fn collected_assets_are_addressed_under_the_package_scope() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("sdk");
        std::fs::create_dir_all(source.join("src")).unwrap();
        std::fs::write(source.join("package.json"), "{}").unwrap();
        std::fs::write(source.join("src/index.ts"), "export const x = 1;").unwrap();

        let mut assets = Vec::new();
        collect_package_assets(&source, std::path::Path::new("@cairn/sdk"), &mut assets).unwrap();

        let mut paths: Vec<&str> = assets.iter().map(|a| a.path.as_str()).collect();
        paths.sort();
        assert_eq!(
            paths,
            ["@cairn/sdk/package.json", "@cairn/sdk/src/index.ts"]
        );
    }
}
