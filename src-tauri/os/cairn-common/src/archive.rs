//! The one way Cairn unpacks an archive it was handed: a headless install's
//! resource bundle, an executor's managed tool trees (`npm`), and a machine
//! service's published artifact (a tar.gz or a zip).
//!
//! Each arrives over the network and is digest-verified before it gets here,
//! but a digest only says the bytes are the ones published, not that the
//! publisher packed nothing hostile. So every entry is held to the shape a
//! published tree has: a relative path that stays under the destination, and a
//! plain file or directory. A symlink, hard link, device, or FIFO is refused by
//! name rather than skipped, since a tree missing an entry its publisher put
//! there is a tree that fails later in a way nobody can trace back here.
//!
//! Permission bits are kept, so a file published executable (npm's
//! `node-gyp-bin/node-gyp`, a shell script npm execs) stays executable;
//! ownership is never applied.

use std::io::Read;
use std::path::{Component, Path};

/// How a published artifact is packed. A declaration that names none ships
/// one file, which is the executable itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ArchiveKind {
    #[serde(rename = "zip")]
    Zip,
    #[serde(rename = "tar.gz")]
    TarGz,
}

impl ArchiveKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zip => "zip",
            Self::TarGz => "tar.gz",
        }
    }
}

/// Unpack the tar.gz `reader` into `destination`, creating it if absent.
/// `what` names the archive in every refusal ("resource bundle", "npm tree").
///
/// A refusal can leave entries already unpacked behind it, so a caller unpacks
/// into a staging directory it removes on failure, never over a live tree.
pub fn unpack_tar_gz(reader: impl Read, destination: &Path, what: &str) -> Result<(), String> {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(reader));
    archive.set_preserve_permissions(false);
    archive.set_unpack_xattrs(false);
    std::fs::create_dir_all(destination)
        .map_err(|error| format!("create {}: {error}", destination.display()))?;
    let entries = archive
        .entries()
        .map_err(|error| format!("{what} is not a tar.gz: {error}"))?;
    for entry in entries {
        let mut entry = entry.map_err(|error| format!("{what} is corrupt: {error}"))?;
        let kind = entry.header().entry_type();
        // Extension headers describe the entry after them (a long path); the
        // reader has already applied them to it.
        if matches!(
            kind,
            tar::EntryType::XGlobalHeader | tar::EntryType::XHeader
        ) {
            continue;
        }
        let path = entry
            .path()
            .map_err(|error| format!("{what} entry has an unreadable path: {error}"))?
            .into_owned();
        let contained = !path.is_absolute()
            && path
                .components()
                .all(|component| matches!(component, Component::Normal(_) | Component::CurDir));
        if !contained {
            return Err(format!("{what} entry {} escapes its root", path.display()));
        }
        if !matches!(kind, tar::EntryType::Regular | tar::EntryType::Directory) {
            return Err(format!(
                "{what} entry {} is a {kind:?}, not a file or directory",
                path.display()
            ));
        }
        // `unpack_in` answers false, rather than failing, for an entry it
        // declined to place (one that would land outside `destination`). The
        // checks above should make that unreachable; if they ever do not, the
        // tree is incomplete, which is a refusal, not a success.
        let placed = entry
            .unpack_in(destination)
            .map_err(|error| format!("unpack {what} entry {}: {error}", path.display()))?;
        if !placed {
            return Err(format!(
                "{what} entry {} was not unpacked: it would land outside {}",
                path.display(),
                destination.display()
            ));
        }
    }
    Ok(())
}

/// Unpack the zip `reader` into `destination`, creating it if absent, under
/// the rules [`unpack_tar_gz`] holds a tar to: every entry a plain file or
/// directory at a relative path that stays under `destination`, a link refused
/// by name. Unix permission bits are kept, so a published executable stays one.
///
/// Entry names are read with `\` as a separator too. The format says `/`, but
/// archivers on Windows write `\`, and Axon's own Windows release does; read
/// literally, such a name is one file called `dir\file.exe` at the top level.
pub fn unpack_zip(
    reader: impl Read + std::io::Seek,
    destination: &Path,
    what: &str,
) -> Result<(), String> {
    let mut archive =
        zip::ZipArchive::new(reader).map_err(|error| format!("{what} is not a zip: {error}"))?;
    std::fs::create_dir_all(destination)
        .map_err(|error| format!("create {}: {error}", destination.display()))?;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("{what} is corrupt: {error}"))?;
        let name = entry.name().replace('\\', "/");
        let path = Path::new(&name);
        let contained = !name.starts_with('/')
            && !path.is_absolute()
            && path
                .components()
                .all(|component| matches!(component, Component::Normal(_) | Component::CurDir));
        if !contained || name.contains(':') {
            return Err(format!("{what} entry {name} escapes its root"));
        }
        if entry.is_symlink() {
            return Err(format!(
                "{what} entry {name} is a symlink, not a file or directory"
            ));
        }
        let target = destination.join(path);
        if entry.is_dir() || name.ends_with('/') {
            std::fs::create_dir_all(&target)
                .map_err(|error| format!("unpack {what} entry {name}: {error}"))?;
            continue;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("unpack {what} entry {name}: {error}"))?;
        }
        let mut file = std::fs::File::create(&target)
            .map_err(|error| format!("unpack {what} entry {name}: {error}"))?;
        std::io::copy(&mut entry, &mut file)
            .map_err(|error| format!("unpack {what} entry {name}: {error}"))?;
        #[cfg(unix)]
        if let Some(mode) = entry.unix_mode() {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode & 0o777))
                .map_err(|error| format!("unpack {what} entry {name}: {error}"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod zip_tests {
    use super::*;
    use std::io::Write;

    fn zipped(build: impl FnOnce(&mut zip::ZipWriter<std::io::Cursor<Vec<u8>>>)) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        build(&mut writer);
        writer.finish().unwrap().into_inner()
    }

    fn file(
        writer: &mut zip::ZipWriter<std::io::Cursor<Vec<u8>>>,
        name: &str,
        mode: u32,
        body: &[u8],
    ) {
        let options = zip::write::SimpleFileOptions::default().unix_permissions(mode);
        writer.start_file(name, options).unwrap();
        writer.write_all(body).unwrap();
    }

    fn unpack(bytes: Vec<u8>) -> (tempfile::TempDir, Result<(), String>) {
        let destination = tempfile::tempdir().unwrap();
        let result = unpack_zip(
            std::io::Cursor::new(bytes),
            &destination.path().join("tree"),
            "axon artifact",
        );
        (destination, result)
    }

    #[test]
    fn an_app_bundle_unpacks_with_its_executable_still_executable() {
        let bytes = zipped(|writer| {
            writer
                .add_directory(
                    "Axon.app/Contents/",
                    zip::write::SimpleFileOptions::default(),
                )
                .unwrap();
            file(
                writer,
                "Axon.app/Contents/Resources/bin/axon",
                0o755,
                b"#!/bin/sh\n",
            );
            file(writer, "Axon.app/Contents/Info.plist", 0o644, b"<plist/>");
        });
        let (root, result) = unpack(bytes);
        result.unwrap();
        let tree = root.path().join("tree");
        assert_eq!(
            std::fs::read(tree.join("Axon.app/Contents/Info.plist")).unwrap(),
            b"<plist/>"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |path: &str| {
                std::fs::metadata(tree.join(path))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777
            };
            assert_eq!(mode("Axon.app/Contents/Resources/bin/axon"), 0o755);
            assert_eq!(mode("Axon.app/Contents/Info.plist") & 0o111, 0);
        }
    }

    /// Axon's Windows release names its entries with `\`.
    #[test]
    fn a_backslash_separated_entry_lands_in_its_directory() {
        let bytes = zipped(|writer| {
            file(
                writer,
                r"axon-win-0.3.6-windows-x86_64\axon-win.exe",
                0o755,
                b"MZ",
            );
        });
        let (root, result) = unpack(bytes);
        result.unwrap();
        assert!(root
            .path()
            .join("tree/axon-win-0.3.6-windows-x86_64/axon-win.exe")
            .is_file());
    }

    #[test]
    fn an_entry_that_climbs_or_is_absolute_is_refused() {
        for name in [
            "../escaped",
            "/etc/escaped",
            "ok/../../escaped",
            r"..\escaped",
            "C:/escaped",
        ] {
            let bytes = zipped(|writer| file(writer, name, 0o644, b"x"));
            let (root, result) = unpack(bytes);
            let refusal = result.unwrap_err();
            assert!(refusal.contains("escapes its root"), "{name}: {refusal}");
            assert!(!root.path().join("escaped").exists());
        }
    }

    #[test]
    fn a_symlink_is_refused_by_name() {
        let bytes = zipped(|writer| {
            writer
                .add_symlink(
                    "Axon.app/Contents/Frameworks/Current",
                    "../../../../etc",
                    zip::write::SimpleFileOptions::default(),
                )
                .unwrap();
        });
        let (_root, result) = unpack(bytes);
        let refusal = result.unwrap_err();
        assert!(
            refusal.contains("Axon.app/Contents/Frameworks/Current") && refusal.contains("symlink"),
            "{refusal}"
        );
    }

    #[test]
    fn bytes_that_are_not_a_zip_are_refused() {
        let (_root, result) = unpack(b"not a zip".to_vec());
        assert!(result
            .unwrap_err()
            .starts_with("axon artifact is not a zip"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gz(build: impl FnOnce(&mut tar::Builder<flate2::write::GzEncoder<Vec<u8>>>)) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        build(&mut builder);
        builder.into_inner().unwrap().finish().unwrap()
    }

    fn file(
        builder: &mut tar::Builder<flate2::write::GzEncoder<Vec<u8>>>,
        path: &str,
        mode: u32,
        body: &[u8],
    ) {
        let mut header = tar::Header::new_gnu();
        header.set_size(body.len() as u64);
        header.set_mode(mode);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder.append_data(&mut header, path, body).unwrap();
    }

    /// A header whose name is written raw, past the builder's own path checks.
    fn raw(
        builder: &mut tar::Builder<flate2::write::GzEncoder<Vec<u8>>>,
        name: &[u8],
        kind: tar::EntryType,
    ) {
        let mut header = tar::Header::new_old();
        header.as_old_mut().name[..name.len()].copy_from_slice(name);
        header.set_size(1);
        header.set_mode(0o644);
        header.set_entry_type(kind);
        header.set_cksum();
        builder.append(&header, &b"x"[..]).unwrap();
    }

    fn unpack(bytes: &[u8]) -> (tempfile::TempDir, Result<(), String>) {
        let destination = tempfile::tempdir().unwrap();
        let result = unpack_tar_gz(bytes, &destination.path().join("tree"), "npm tree");
        (destination, result)
    }

    #[test]
    fn files_and_directories_unpack_with_their_contents() {
        let bytes = gz(|builder| {
            file(builder, "node_modules/npm/package.json", 0o644, b"{}");
            file(
                builder,
                "node_modules/corepack/dist/corepack.js",
                0o644,
                b"//",
            );
        });
        let (root, result) = unpack(&bytes);
        result.unwrap();
        let tree = root.path().join("tree");
        assert_eq!(
            std::fs::read(tree.join("node_modules/npm/package.json")).unwrap(),
            b"{}"
        );
        assert!(tree
            .join("node_modules/corepack/dist/corepack.js")
            .is_file());
    }

    /// npm execs `node-gyp-bin/node-gyp` itself; a tree that lost its execute
    /// bit on the way in would fail every native build, far from here.
    #[cfg(unix)]
    #[test]
    fn an_executable_file_stays_executable() {
        use std::os::unix::fs::PermissionsExt;
        let bytes = gz(|builder| {
            file(builder, "node-gyp-bin/node-gyp", 0o755, b"#!/bin/sh\n");
            file(builder, "package.json", 0o644, b"{}");
        });
        let (root, result) = unpack(&bytes);
        result.unwrap();
        let mode = |path: &str| {
            std::fs::metadata(root.path().join("tree").join(path))
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(mode("node-gyp-bin/node-gyp"), 0o755);
        assert_eq!(mode("package.json") & 0o111, 0);
    }

    /// The deterministic packer writes a PAX record for npm's deepest paths.
    #[test]
    fn a_long_path_carried_by_an_extension_header_unpacks_whole() {
        let long = format!("{}leaf.js", "deep/".repeat(30));
        let bytes = gz(|builder| file(builder, &long, 0o644, b"1"));
        let (root, result) = unpack(&bytes);
        result.unwrap();
        assert!(root.path().join("tree").join(&long).is_file());
    }

    #[test]
    fn an_entry_that_climbs_or_is_absolute_is_refused() {
        for name in [&b"../escaped"[..], b"/etc/escaped", b"ok/../../escaped"] {
            let bytes = gz(|builder| raw(builder, name, tar::EntryType::Regular));
            let (root, result) = unpack(&bytes);
            let refusal = result.unwrap_err();
            assert!(refusal.contains("escapes its root"), "{refusal}");
            assert!(refusal.starts_with("npm tree entry"), "{refusal}");
            assert!(!root.path().join("escaped").exists());
        }
    }

    #[test]
    fn a_link_of_either_kind_is_refused_by_name() {
        for kind in [tar::EntryType::Symlink, tar::EntryType::Link] {
            let bytes = gz(|builder| {
                let mut header = tar::Header::new_gnu();
                header.set_size(0);
                header.set_mode(0o777);
                header.set_entry_type(kind);
                builder
                    .append_link(
                        &mut header,
                        "node_modules/.bin/semver",
                        "../semver/bin/semver.js",
                    )
                    .unwrap();
            });
            let (root, result) = unpack(&bytes);
            let refusal = result.unwrap_err();
            assert!(
                refusal.contains("node_modules/.bin/semver")
                    && refusal.contains("not a file or directory"),
                "{refusal}"
            );
            assert!(!root.path().join("tree/node_modules/.bin/semver").exists());
        }
    }

    #[test]
    fn bytes_that_are_not_a_tar_gz_are_refused() {
        let (_root, result) = unpack(b"not gzip at all");
        let refusal = result.unwrap_err();
        assert!(refusal.starts_with("npm tree is"), "{refusal}");
    }
}
