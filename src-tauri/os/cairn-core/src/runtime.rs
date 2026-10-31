//! The runner's own copy of the Cairn JavaScript packages, and how it reaches
//! the machine that runs a batch.
//!
//! [`cairn_common::runtime`] owns WHERE these packages install and why placement
//! rather than a resolver hook is the mechanism. This module owns WHAT is
//! installed: the version-matched `@cairn/sdk` that ships with this build, read
//! from the runtime the app provisions into `<CAIRN_HOME>/runtime` (or, in a
//! development build, straight from the checkout it was compiled from).
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
/// reads the checkout it was compiled from first, because that is the only copy
/// guaranteed to match it: a runtime provisioned under `CAIRN_HOME` by some
/// earlier app launch is whatever that launch staged, and a debug binary or test
/// that shipped it would exercise an SDK nobody is reviewing.
pub fn runtime_package(name: &str) -> Result<PathBuf, String> {
    #[cfg(debug_assertions)]
    if let Some(checkout) = checkout_runtime_package(name) {
        return Ok(checkout);
    }
    if let Some(installed) = installed_runtime_package(&cairn_common::paths::cairn_home(), name) {
        return Ok(installed);
    }
    Err(format!(
        "Cairn runtime package {SCOPE}/{name} is not installed under CAIRN_HOME/runtime/node_modules"
    ))
}

/// The package in the checkout this binary runs from, found at run time rather
/// than baked in: see [`cairn_common::paths::development_checkout_ancestor`].
#[cfg(debug_assertions)]
fn checkout_runtime_package(name: &str) -> Option<PathBuf> {
    let package = |repo: &Path| repo.join("packages").join(name);
    cairn_common::paths::development_checkout_ancestor(|repo| {
        package(repo).join("package.json").is_file()
    })
    .map(|repo| package(&repo))
}

fn installed_runtime_package(home: &Path, name: &str) -> Option<PathBuf> {
    let installed = home.join("runtime/node_modules/@cairn").join(name);
    installed
        .join("package.json")
        .is_file()
        .then_some(installed)
}

/// The files a rendered page read hands bun, built by `scripts/build-page-render.ts`.
pub const PAGE_RENDER_HOST: &str = "host.js";
pub const PAGE_RENDER_KIT: &str = "page-kit.text.js";

/// The directory holding the page render bundle (the text page kit and the
/// headless host).
///
/// A release build reads the copy the app provisioned into
/// `<CAIRN_HOME>/runtime/page-render`. A development build reads its own
/// checkout's `src-tauri/runtime/page-render`, rebuilding it first when its
/// inputs changed since it was stamped, so a kit edit shows in the next
/// rendered read and a test renders with the kit it is reviewing.
pub fn page_render_bundle() -> Result<PathBuf, String> {
    #[cfg(debug_assertions)]
    if let Some(repo) = cairn_common::paths::development_checkout_ancestor(|repo| {
        repo.join("scripts/build-page-render.ts").is_file()
    }) {
        return checkout_page_render_bundle(&repo);
    }
    let installed = cairn_common::paths::cairn_home().join("runtime/page-render");
    if page_render_bundle_complete(&installed) {
        return Ok(installed);
    }
    Err(format!(
        "the page render bundle is not installed at {}; reinstall Cairn, or read the page with ?format=json for its source",
        installed.display()
    ))
}

fn page_render_bundle_complete(dir: &Path) -> bool {
    [PAGE_RENDER_HOST, PAGE_RENDER_KIT]
        .iter()
        .all(|file| dir.join(file).is_file())
}

/// Build the checkout's bundle if it is stale, one build at a time per process.
#[cfg(debug_assertions)]
fn checkout_page_render_bundle(repo: &Path) -> Result<PathBuf, String> {
    static BUILD: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = BUILD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let out = repo.join("src-tauri/runtime/page-render");
    let bun = crate::env::resolve_sidecar("bun");
    let output = std::process::Command::new(&bun)
        .arg(repo.join("scripts/build-page-render.ts"))
        .arg("--if-stale")
        .arg("--out")
        .arg(&out)
        .current_dir(repo)
        .output()
        .map_err(|error| format!("could not run bun to build the page render bundle: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "building the page render bundle failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    if !page_render_bundle_complete(&out) {
        return Err(format!(
            "the page render build finished without writing {}",
            out.display()
        ));
    }
    Ok(out)
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

    /// A development build ships the SDK from its own checkout, whatever a
    /// previous app launch provisioned under `CAIRN_HOME`. The expected path is
    /// spelled relative to the manifest rather than through the same ancestor
    /// walk, so an off-by-one in that walk cannot agree with itself.
    #[cfg(debug_assertions)]
    #[test]
    fn a_development_build_ships_the_sdk_from_its_own_checkout() {
        let expected = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../packages/sdk")
            .canonicalize()
            .expect("the repository carries packages/sdk");
        let resolved = runtime_package("sdk").expect("a development build resolves the SDK");
        assert_eq!(resolved.canonicalize().unwrap(), expected);
        assert!(
            sdk_packages()
                .unwrap()
                .iter()
                .any(|asset| asset.path == "@cairn/sdk/src/client.ts"),
            "the shipped assets must be the checkout's SDK source"
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
