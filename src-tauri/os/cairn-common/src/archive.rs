//! The one way Cairn unpacks a gzip-compressed tar it was handed: a headless
//! install's resource bundle, and an executor's managed tool trees (`npm`).
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
