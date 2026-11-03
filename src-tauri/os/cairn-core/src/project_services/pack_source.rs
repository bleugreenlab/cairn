//! Service declarations an installed pack ships: `services/<id>/service.yaml`
//! in the pack's source. A pack-owned service is the one kind that may run one
//! resident per machine (`placement: per-machine`); which machines is the
//! operator's fleet row, `buildSlots.machineServices`.
//!
//! Only the declaration is read here. Pack sync materializes the item kinds it
//! knows into the workspace, and a service is not yet one of them, so the
//! source of truth is the installed pack's own source tree. Today that tree is
//! known for a bundled pack (the app's resource directory); a pack installed
//! from a URL or a local directory lists no services until pack sync carries
//! them.

use std::path::{Path, PathBuf};

use super::declaration::{
    package_from_files_in, parse_service_manifest_in, PackageOrigin, Placement, ServiceListing,
    ServicePackage, ServiceSource, MANIFEST_FILE, PACK_SERVICES_DIR,
};
use cairn_config::fleet::MachineServiceKey;

/// One installed pack's service declarations.
pub(crate) struct PackServiceSource {
    pack: String,
    revision: String,
    /// `None` when the pack is not installed or its source is not readable
    /// here, which lists nothing: every cell it owned drains.
    root: Option<PathBuf>,
}

impl PackServiceSource {
    /// The pack `pack` as this workspace has it installed.
    pub(crate) fn open(config_dir: &Path, pack: &str) -> Self {
        let lock = installed_lock(config_dir, pack);
        Self {
            pack: pack.to_string(),
            revision: lock.as_ref().map_or_else(
                || format!("{pack} (not installed)"),
                |lock| format!("{} {}", lock.id, lock.version),
            ),
            root: lock.and_then(|lock| source_root(config_dir, &lock)),
        }
    }

    /// A pack whose source is the directory `root`.
    #[cfg(test)]
    pub(crate) fn at_root(pack: &str, root: PathBuf) -> Self {
        Self {
            pack: pack.to_string(),
            revision: format!("{pack} (test)"),
            root: Some(root),
        }
    }

    /// Every installed pack, by id.
    pub(crate) fn installed(config_dir: &Path) -> Vec<Self> {
        cairn_packs::lock::installed_packs(config_dir)
            .into_iter()
            .filter(|lock| !cairn_packs::lock::is_uninstalled(config_dir, &lock.id))
            .map(|lock| Self {
                revision: format!("{} {}", lock.id, lock.version),
                root: source_root(config_dir, &lock),
                pack: lock.id,
            })
            .collect()
    }

    pub(crate) fn pack(&self) -> &str {
        &self.pack
    }

    fn services_dir(&self) -> Option<PathBuf> {
        self.root.as_ref().map(|root| root.join(PACK_SERVICES_DIR))
    }

    fn manifest_bytes(&self, id: &str) -> Result<Option<Vec<u8>>, String> {
        let Some(dir) = self.services_dir() else {
            return Ok(None);
        };
        let path = dir.join(id).join(MANIFEST_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("read {}: {error}", path.display())),
        }
    }
}

fn installed_lock(config_dir: &Path, pack: &str) -> Option<cairn_packs::PackLock> {
    if cairn_packs::lock::is_uninstalled(config_dir, pack) {
        return None;
    }
    cairn_packs::lock::read_lock(config_dir, pack)
}

/// Where an installed pack's source tree is on this machine.
fn source_root(config_dir: &Path, lock: &cairn_packs::PackLock) -> Option<PathBuf> {
    match lock.source.kind {
        cairn_packs::lock::PackSourceKind::Bundled => cairn_packs::source_dir(config_dir)
            .and_then(|resources| cairn_packs::available_pack(&resources, &lock.id))
            .map(|manifest| manifest.root),
        cairn_packs::lock::PackSourceKind::Url | cairn_packs::lock::PackSourceKind::Local => None,
    }
}

impl ServiceSource for PackServiceSource {
    fn revision(&self) -> &str {
        &self.revision
    }

    fn list(&self) -> Result<Vec<ServiceListing>, String> {
        let Some(dir) = self.services_dir() else {
            return Ok(Vec::new());
        };
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(format!("read {}: {error}", dir.display())),
        };
        let mut listings = Vec::new();
        for entry in entries.filter_map(Result::ok) {
            if !entry.path().is_dir() {
                continue;
            }
            let Some(id) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            // A directory without a manifest is not a service.
            let Some(bytes) = self.manifest_bytes(&id)? else {
                continue;
            };
            let origin = PackageOrigin::pack(&self.pack, &id);
            let declaration = match std::str::from_utf8(&bytes) {
                Ok(text) => parse_service_manifest_in(&origin, &id, text),
                Err(_) => Err(format!(
                    "pack {}: {id}/{MANIFEST_FILE} is not UTF-8",
                    self.pack
                )),
            };
            listings.push(ServiceListing { id, declaration });
        }
        listings.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(listings)
    }

    fn package(&self, id: &str) -> Result<Option<ServicePackage>, String> {
        if !super::declaration::is_valid_service_id(id) {
            return Ok(None);
        }
        let Some(bytes) = self.manifest_bytes(id)? else {
            return Ok(None);
        };
        package_from_files_in(
            &PackageOrigin::pack(&self.pack, id),
            id,
            vec![(MANIFEST_FILE.to_string(), bytes)],
        )
        .map(Some)
    }
}

/// One machine service an installed pack declares, by `<pack>/<service>`.
pub(crate) struct DeclaredMachineService {
    pub key: MachineServiceKey,
    /// The declaration, or why it does not parse: an invalid declaration is
    /// listed rather than hidden, so status can say why nothing runs.
    pub package: Result<ServicePackage, String>,
}

/// Every per-machine service the installed packs declare, in key order.
pub(crate) fn declared_machine_services(config_dir: &Path) -> Vec<DeclaredMachineService> {
    let mut declared = Vec::new();
    for source in PackServiceSource::installed(config_dir) {
        declared.extend(machine_services_in(&source));
    }
    declared.sort_by(|a, b| a.key.cmp(&b.key));
    declared
}

fn machine_services_in(source: &PackServiceSource) -> Vec<DeclaredMachineService> {
    let listings = match source.list() {
        Ok(listings) => listings,
        Err(error) => {
            tracing::warn!(pack = source.pack(), %error, "could not list a pack's services");
            return Vec::new();
        }
    };
    listings
        .into_iter()
        .filter_map(|listing| {
            let key = MachineServiceKey::new(source.pack(), &listing.id).ok()?;
            let package = match listing.declaration {
                Ok(declaration) if declaration.placement == Placement::PerMachine => source
                    .package(&listing.id)
                    .and_then(|package| package.ok_or_else(|| "its declaration vanished".into())),
                // A single-instance pack service is not a machine service.
                Ok(_) => return None,
                Err(error) => Err(error),
            };
            Some(DeclaredMachineService { key, package })
        })
        .collect()
}

/// The declaration of `key`, if an installed pack ships it.
pub(crate) fn declared_machine_service(
    config_dir: &Path,
    key: &MachineServiceKey,
) -> Option<DeclaredMachineService> {
    machine_services_in(&PackServiceSource::open(config_dir, &key.pack))
        .into_iter()
        .find(|declared| &declared.key == key)
}

/// The hash a native package's bytes give, for a test's expectation.
#[cfg(test)]
pub(crate) fn manifest_hash(bytes: &[u8]) -> String {
    super::declaration::source_hash(&[(MANIFEST_FILE.to_string(), bytes.to_vec())])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_services::declaration::{Callback, Transport};

    const AXON: &str = include_str!("../../../../packs/axon/services/axon/service.yaml");

    fn pack_with(services: &[(&str, &str)]) -> (tempfile::TempDir, PackServiceSource) {
        let dir = tempfile::tempdir().unwrap();
        for (id, manifest) in services {
            let at = dir.path().join("services").join(id);
            std::fs::create_dir_all(&at).unwrap();
            std::fs::write(at.join("service.yaml"), manifest).unwrap();
        }
        std::fs::create_dir_all(dir.path().join("services/notes")).unwrap();
        let source = PackServiceSource::at_root("axon", dir.path().to_path_buf());
        (dir, source)
    }

    /// The declaration this repository ships must stay valid, and say what
    /// the published artifacts are.
    #[test]
    fn the_bundled_axon_declaration_parses() {
        let (_dir, source) = pack_with(&[("axon", AXON)]);
        let package = source.package("axon").unwrap().unwrap();
        let declaration = &package.declaration;
        assert_eq!(declaration.transport, Transport::McpStdio);
        assert_eq!(declaration.callback, Callback::None);
        assert_eq!(declaration.placement, Placement::PerMachine);
        let native = declaration.native().unwrap();
        assert_eq!(native.args, vec!["mcp".to_string()]);
        assert_eq!(native.version, "0.3.6");
        assert_eq!(
            native.targets.keys().collect::<Vec<_>>(),
            [
                "aarch64-apple-darwin",
                "x86_64-pc-windows-msvc",
                "x86_64-unknown-linux-gnu"
            ]
        );
        assert_eq!(
            native.targets["aarch64-apple-darwin"].executable,
            "Axon.app/Contents/Resources/bin/axon"
        );
        assert_eq!(native.probe.every, std::time::Duration::from_secs(300));
        assert_eq!(package.source_hash, manifest_hash(AXON.as_bytes()));
    }

    #[test]
    fn a_pack_lists_its_services_and_skips_directories_without_a_manifest() {
        let (_dir, source) = pack_with(&[
            ("axon", AXON),
            ("broken", "name: Broken\nruntime: native\n"),
        ]);
        let listed = source.list().unwrap();
        let ids: Vec<&str> = listed.iter().map(|listing| listing.id.as_str()).collect();
        assert_eq!(ids, ["axon", "broken"]);
        assert!(listed[0].declaration.is_ok());
        let error = listed[1].declaration.as_ref().unwrap_err();
        assert!(
            error.contains("pack axon: services/broken/service.yaml"),
            "{error}"
        );
        let declared = machine_services_in(&source);
        assert_eq!(
            declared.len(),
            2,
            "an invalid declaration is listed with its error"
        );
        assert_eq!(declared[0].key.to_string(), "axon/axon");
        assert!(source.package("notes").unwrap().is_none());
    }

    #[test]
    fn a_pack_that_is_not_installed_lists_nothing() {
        let home = tempfile::tempdir().unwrap();
        let source = PackServiceSource::open(home.path(), "axon");
        assert!(source.list().unwrap().is_empty());
        assert!(source.package("axon").unwrap().is_none());
        assert!(declared_machine_services(home.path()).is_empty());
    }
}
