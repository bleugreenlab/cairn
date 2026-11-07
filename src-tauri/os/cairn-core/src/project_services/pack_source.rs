//! Service declarations an installed pack ships: `services/<id>/service.yaml`
//! in the pack's source. A pack-owned service is the one kind that may be a
//! machine service: one resident per machine (`placement: per-machine`), where
//! which machines is the operator's fleet row, `buildSlots.machineServices`; or
//! delivered to every machine and launched on demand (`placement: fleet`),
//! where the row holds only per-machine overrides.
//!
//! Only the declaration is read here, and always from the app's own bundle. A
//! service is never mirrored into the config directory, which agents can write,
//! because a declaration names artifact URLs, hashes and a command. The lock
//! decides what is declared: an item recorded as removed is not. A per-machine
//! override runs the shipped declaration with the operator's own binary. A pack
//! installed from a URL or a local directory declares no services.

use std::path::{Path, PathBuf};

use super::declaration::{
    package_from_files_in, parse_service_manifest_in, PackageOrigin, Placement, ServiceListing,
    ServicePackage, ServiceSource, MANIFEST_FILE, PACK_SERVICES_DIR,
};
use cairn_config::fleet::MachineServiceKey;
use cairn_packs::PackItemKind;

/// One installed pack's service declarations.
pub(crate) struct PackServiceSource {
    pack: String,
    revision: String,
    /// `None` when the pack is not installed or its source is not readable
    /// here, which lists nothing: every cell it owned drains.
    root: Option<PathBuf>,
    /// The workspace and the lock that decide which of the source's services
    /// are declared. `None` only for a source a test builds from a directory.
    installed: Option<(PathBuf, cairn_packs::PackLock)>,
}

impl PackServiceSource {
    /// The pack `pack` as this workspace has it installed.
    pub(crate) fn open(config_dir: &Path, pack: &str) -> Self {
        match installed_lock(config_dir, pack) {
            Some(lock) => Self::of_lock(config_dir, lock),
            None => Self {
                pack: pack.to_string(),
                revision: format!("{pack} (not installed)"),
                root: None,
                installed: None,
            },
        }
    }

    fn of_lock(config_dir: &Path, lock: cairn_packs::PackLock) -> Self {
        Self {
            pack: lock.id.clone(),
            revision: format!("{} {}", lock.id, lock.version),
            root: source_root(config_dir, &lock),
            installed: Some((config_dir.to_path_buf(), lock)),
        }
    }

    /// A pack whose source is the directory `root`.
    #[cfg(test)]
    pub(crate) fn at_root(pack: &str, root: PathBuf) -> Self {
        Self {
            pack: pack.to_string(),
            revision: format!("{pack} (test)"),
            root: Some(root),
            installed: None,
        }
    }

    /// Every installed pack, by id.
    pub(crate) fn installed(config_dir: &Path) -> Vec<Self> {
        cairn_packs::lock::installed_packs(config_dir)
            .into_iter()
            .filter(|lock| !cairn_packs::lock::is_uninstalled(config_dir, &lock.id))
            .map(|lock| Self::of_lock(config_dir, lock))
            .collect()
    }

    pub(crate) fn pack(&self) -> &str {
        &self.pack
    }

    fn services_dir(&self) -> Option<PathBuf> {
        self.root.as_ref().map(|root| root.join(PACK_SERVICES_DIR))
    }

    /// Whether the user took the service `id` out of this pack.
    fn is_removed(&self, id: &str) -> bool {
        self.installed
            .as_ref()
            .is_some_and(|(_, lock)| lock.is_removed(PackItemKind::Service, id))
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
            if self.is_removed(&id) {
                continue;
            }
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
        if self.is_removed(id) {
            return Ok(None);
        }
        package_from_files_in(
            &PackageOrigin::pack(&self.pack, id),
            id,
            vec![(MANIFEST_FILE.to_string(), bytes)],
        )
        .map(Some)
    }

    /// A pack's service runs from the app bundle's own declaration, which
    /// nothing in the workspace can edit, so its artifact is never refused.
    fn refusal(&self, _id: &str) -> Option<String> {
        None
    }
}

/// One machine service an installed pack declares, by `<pack>/<service>`.
pub(crate) struct DeclaredMachineService {
    pub key: MachineServiceKey,
    /// The declaration, or why it does not parse: an invalid declaration is
    /// listed rather than hidden, so status can say why nothing runs.
    pub package: Result<ServicePackage, String>,
    /// Why the pack's own artifact may not run; see [`ServiceSource::refusal`].
    pub refusal: Option<String>,
}

/// Every machine service (per-machine or fleet) the installed packs declare, in
/// key order.
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
                Ok(declaration) if declaration.placement != Placement::Single => source
                    .package(&listing.id)
                    .and_then(|package| package.ok_or_else(|| "its declaration vanished".into())),
                // A single-instance pack service is not a machine service.
                Ok(_) => return None,
                Err(error) => Err(error),
            };
            let refusal = source.refusal(&listing.id);
            Some(DeclaredMachineService {
                key,
                package,
                refusal,
            })
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
        assert_eq!(
            native.resident_probe().unwrap().every,
            std::time::Duration::from_secs(300)
        );
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

    /// `pack` installed from a bundle shipping one service: (home, resources).
    fn installed_pack(dir: &Path, manifest: &str) -> (PathBuf, PathBuf) {
        let (home, resources) = (dir.join("home"), dir.join("resources"));
        let root = resources.join("packs/kit");
        std::fs::create_dir_all(root.join("services/probe")).unwrap();
        std::fs::write(
            root.join("cairn-pack.yaml"),
            "id: kit\nname: Kit\nversion: 1.0.0\n",
        )
        .unwrap();
        std::fs::write(root.join("services/probe/service.yaml"), manifest).unwrap();
        cairn_packs::sync_one_pack(
            &crate::services::RealGitClient,
            &crate::services::RealFileSystem,
            &resources,
            &home,
            "kit",
        )
        .unwrap();
        (home, resources)
    }

    fn key() -> MachineServiceKey {
        "kit/probe".parse().unwrap()
    }

    #[test]
    fn a_service_removed_from_its_pack_is_not_declared() {
        let dir = tempfile::tempdir().unwrap();
        let (home, _) = installed_pack(dir.path(), AXON);
        assert!(declared_machine_service(&home, &key()).is_some());
        assert!(
            !home.join("services/probe").exists(),
            "a service is never mirrored into the workspace"
        );

        cairn_packs::note_removed_item(&home, PackItemKind::Service, "probe");
        assert!(declared_machine_service(&home, &key()).is_none());
        let source = PackServiceSource::open(&home, "kit");
        assert!(source.list().unwrap().is_empty());
        assert!(source.package("probe").unwrap().is_none());
    }

    /// A workspace that installed the pack before it shipped this service has
    /// a lock with no item for it. The service is declared all the same, from
    /// the bundle and unrefused, so delivery does not wait for a sync; the next
    /// sync records the item and still writes nothing into the workspace.
    #[test]
    fn a_lock_from_before_the_pack_shipped_the_service_still_declares_it() {
        let dir = tempfile::tempdir().unwrap();
        let (home, resources) = installed_pack(dir.path(), AXON);
        let mut lock = cairn_packs::lock::read_lock(&home, "kit").unwrap();
        lock.items.retain(|item| item.kind != PackItemKind::Service);
        cairn_packs::lock::write_lock(&crate::services::RealFileSystem, &home, &lock).unwrap();

        let declared = declared_machine_service(&home, &key()).expect("declared from the bundle");
        assert!(declared.refusal.is_none());
        assert!(declared.package.is_ok());

        cairn_packs::sync_one_pack(
            &crate::services::RealGitClient,
            &crate::services::RealFileSystem,
            &resources,
            &home,
            "kit",
        )
        .unwrap();
        assert!(cairn_packs::lock::read_lock(&home, "kit")
            .unwrap()
            .item(PackItemKind::Service, "probe")
            .is_some());
        assert!(!home.join("services/probe").exists());
        assert!(declared_machine_service(&home, &key())
            .unwrap()
            .refusal
            .is_none());
    }

    #[test]
    fn a_service_from_a_non_bundled_source_is_never_declared() {
        let dir = tempfile::tempdir().unwrap();
        let (home, _) = installed_pack(dir.path(), AXON);
        let mut lock = cairn_packs::lock::read_lock(&home, "kit").unwrap();
        lock.source = cairn_packs::PackSource::local("elsewhere".to_string());
        cairn_packs::lock::write_lock(&crate::services::RealFileSystem, &home, &lock).unwrap();
        assert!(declared_machine_service(&home, &key()).is_none());
    }

    #[test]
    fn a_bundled_declaration_without_a_size_is_listed_with_an_error_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let sizeless: String = AXON
            .lines()
            .filter(|line| !line.trim_start().starts_with("size:"))
            .map(|line| format!("{line}\n"))
            .collect();
        let (home, _) = installed_pack(dir.path(), &sizeless);
        let declared = declared_machine_service(&home, &key()).expect("listed with its error");
        let Err(error) = declared.package else {
            panic!("a declaration without a size must be refused");
        };
        assert!(error.contains("`size`"), "{error}");
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
