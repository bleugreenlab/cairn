//! Resource packs: the unit of offering and installing Cairn resources.
//!
//! A pack is a **source-side** grouping. Installing one copies its contents into
//! the flat `~/.cairn` layout (`agents/`, `recipes/`, `responses/`, `skills/`,
//! `workflows/`) that discovery and global ids already assume — packs do not
//! namespace what they install. That is what makes repacking the shipped tree a
//! pure file move: an existing workspace's files already sit at exactly the
//! paths a pack-aware sync would write, so the migration writes nothing.
//!
//! - [`manifest`] describes a pack at its source (native `cairn-pack.yaml`, an
//!   ingested Claude Code plugin, a bare `SKILL.md`, or bare conventions).
//! - [`lock`] records what an install put in the workspace, at what version,
//!   from where.
//! - [`mcp`] carries a pack's MCP server definitions and the layer they form
//!   beneath the user's own settings.

pub mod lock;
pub mod manifest;
pub mod mcp;

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

pub use lock::{installed_packs, PackLock, PackLockItem, PackSource, PackSourceKind};
pub use manifest::{PackFormat, PackItem, PackItemKind, PackManifest};

use crate::config::mcp_servers::McpServerConfig;

/// Hashing distinguishes absent content from a present empty file or tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentHash {
    Missing,
    Present(String),
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn is_canonical_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn hash_file(path: &Path) -> Result<ContentHash, String> {
    if !path.exists() {
        return Ok(ContentHash::Missing);
    }
    let bytes =
        std::fs::read(path).map_err(|e| format!("Failed to read resource {path:?}: {e}"))?;
    Ok(ContentHash::Present(sha256_bytes(&bytes)))
}

pub fn hash_tree(root: &Path) -> Result<ContentHash, String> {
    if !root.exists() {
        return Ok(ContentHash::Missing);
    }
    Ok(ContentHash::Present(hash_tree_present(root)?))
}

pub fn hash_item_path(kind: PackItemKind, path: &Path) -> Result<ContentHash, String> {
    match kind {
        PackItemKind::Skill | PackItemKind::Workflow => hash_tree(path),
        PackItemKind::Mcp => Err("MCP items must be hashed from their server definition".into()),
        PackItemKind::Agent | PackItemKind::Recipe | PackItemKind::Response => hash_file(path),
    }
}

/// Hash one MCP definition after recursively sorting every map key.
pub fn hash_mcp_definition(config: &McpServerConfig) -> Result<String, String> {
    let value = serde_json::to_value(config)
        .map_err(|e| format!("Failed to serialize MCP server definition: {e}"))?;
    let canonical = canonical_json(value);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|e| format!("Failed to serialize MCP server definition: {e}"))?;
    Ok(sha256_bytes(&bytes))
}

fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => {
            let sorted = values
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<std::collections::BTreeMap<_, _>>();
            serde_json::to_value(sorted).expect("JSON values always serialize")
        }
        other => other,
    }
}

/// Content directories a pack may ship, and the exact subtrees a workspace
/// install materializes. Flat dirs (`agents`, `recipes`, `responses`) are
/// per-file copy-when-missing with in-place pack updates; package dirs
/// (`skills`, `workflows`) are whole-directory copy-when-missing and never
/// overwritten, so a user's edited package is preserved.
pub const CONTENT_DIRS: [&str; 5] = ["agents", "recipes", "responses", "skills", "workflows"];

/// Subdirectory of the app's resource directory holding shipped packs.
pub const PACKS_SOURCE_DIR: &str = "packs";

/// Machine-local record of the app resource directory the last sync ran from.
///
/// Only the desktop resolves the app bundle's resource directory, but the runner
/// — which owns every `cairn://` read and write — needs it to answer "what does
/// this app ship?". Recording it here is how the catalog resource finds the
/// available packs without new cross-process plumbing. It is a path on THIS
/// machine, so it lives outside the workspace repo's tracked allowlist.
const SOURCE_DIR_MARKER: &str = ".pack-source";

/// Record `resource_dir` as this workspace's pack source. Written only when it
/// changes, so a re-sync does not touch the file.
pub fn record_source_dir(
    fs: &dyn crate::services::FileSystem,
    resource_dir: &Path,
    config_dir: &Path,
) -> Result<(), String> {
    let marker = config_dir.join(SOURCE_DIR_MARKER);
    let value = resource_dir.to_string_lossy().to_string();
    let current = fs
        .exists(&marker)
        .then(|| fs.read_to_string(&marker))
        .transpose()?;
    if current.as_deref().map(str::trim) == Some(value.as_str()) {
        return Ok(());
    }
    fs.write_str(&marker, &format!("{value}\n"))
}

/// The app resource directory the last sync of `config_dir` ran from, if it is
/// still present on this machine.
pub fn source_dir(config_dir: &Path) -> Option<PathBuf> {
    let recorded = std::fs::read_to_string(config_dir.join(SOURCE_DIR_MARKER)).ok()?;
    let path = PathBuf::from(recorded.trim());
    path.join(PACKS_SOURCE_DIR).is_dir().then_some(path)
}

/// Every pack shipped in `resource_dir/packs/`, sorted by id.
///
/// A directory that fails to resolve as a pack is logged and skipped rather than
/// failing startup: one malformed pack must not cost a user the rest of them.
pub fn discover_available_packs(resource_dir: &Path) -> Vec<PackManifest> {
    let root = resource_dir.join(PACKS_SOURCE_DIR);
    let Ok(entries) = std::fs::read_dir(&root) else {
        log::debug!("No shipped packs at {root:?}");
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    dirs.sort();

    let mut packs = Vec::new();
    for dir in dirs {
        match manifest::load(&dir) {
            Ok(pack) => packs.push(pack),
            Err(error) => log::warn!("Skipping shipped pack {dir:?}: {error}"),
        }
    }
    packs.sort_by(|left, right| left.id.cmp(&right.id));
    packs
}

/// Record that a workspace item the user just deleted came from an installed
/// pack, so the next sync does not copy it straight back.
///
/// Best-effort and side-effect-only: an item no installed pack claims is simply
/// the user's own content, and a lock that cannot be written is logged rather
/// than failing a deletion the filesystem has already performed.
pub fn note_removed_item(config_dir: &Path, kind: PackItemKind, id: &str) {
    if let Some(pack_id) =
        lock::record_item_removal(&crate::services::RealFileSystem, config_dir, kind, id)
    {
        log::info!("Recorded removal of {kind:?} `{id}` from installed pack `{pack_id}`");
    }
}

/// One shipped pack by id, if present.
pub fn available_pack(resource_dir: &Path, id: &str) -> Option<PackManifest> {
    discover_available_packs(resource_dir)
        .into_iter()
        .find(|pack| pack.id == id)
}

/// Content hash of every file under `root`, path-and-length framed so a rename
/// cannot collide with an edit. This is the per-pack replacement for the old
/// single global `.bundle-sync` marker: an unchanged hash short-circuits that
/// pack's sync entirely.
pub fn content_hash(root: &Path) -> Result<String, String> {
    if !root.exists() {
        return hash_tree_present(root);
    }
    hash_tree_present(root)
}

fn hash_tree_present(root: &Path) -> Result<String, String> {
    let mut entries = Vec::new();
    if root.exists() {
        collect_files(root, root, &mut entries)?;
    }
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));

    let mut hasher = Sha256::new();
    for (relative_path, bytes) in entries {
        let path_bytes = relative_path.as_bytes();
        let file_hash = sha256_bytes(&bytes);
        hasher.update((path_bytes.len() as u64).to_le_bytes());
        hasher.update(path_bytes);
        hasher.update((file_hash.len() as u64).to_le_bytes());
        hasher.update(file_hash.as_bytes());
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn collect_files(
    root: &Path,
    dir: &Path,
    entries: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), String> {
    let mut children = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read resource directory {dir:?}: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read resource entry: {e}"))?;
    children.sort_by_key(|entry| entry.path());

    for child in children {
        let path = child.path();
        let file_type = child
            .file_type()
            .map_err(|e| format!("Failed to inspect resource {path:?}: {e}"))?;
        if file_type.is_dir() {
            collect_files(root, &path, entries)?;
        } else if file_type.is_file() {
            let relative_path = path
                .strip_prefix(root)
                .map_err(|e| format!("Failed to relativize resource {path:?}: {e}"))?
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let bytes = std::fs::read(&path)
                .map_err(|e| format!("Failed to read resource {path:?}: {e}"))?;
            entries.push((relative_path, bytes));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn shipped_packs_are_discovered_sorted_and_bad_ones_skipped() {
        let temp = TempDir::new().unwrap();
        let resources = temp.path();
        write(
            &resources.join("packs/matlab/cairn-pack.yaml"),
            "id: matlab\nname: MATLAB\nversion: 1.0.0\n",
        );
        write(
            &resources.join("packs/core/cairn-pack.yaml"),
            "id: core\nname: Core\nversion: 1.0.0\ndefault: true\n",
        );
        std::fs::create_dir_all(resources.join("packs/junk/docs")).unwrap();

        let packs = discover_available_packs(resources);
        assert_eq!(
            packs.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(),
            vec!["core", "matlab"]
        );
        assert!(packs[0].default);
        assert!(!packs[1].default);
        assert_eq!(available_pack(resources, "matlab").unwrap().name, "MATLAB");
        assert!(available_pack(resources, "linear").is_none());
    }

    #[test]
    fn content_hash_tracks_content_and_paths() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("pack");
        write(&root.join("agents/a.md"), "one\n");
        let first = content_hash(&root).unwrap();

        write(&root.join("agents/a.md"), "two\n");
        assert_ne!(first, content_hash(&root).unwrap());

        write(&root.join("agents/a.md"), "one\n");
        assert_eq!(first, content_hash(&root).unwrap());

        std::fs::rename(root.join("agents/a.md"), root.join("agents/b.md")).unwrap();
        assert_ne!(
            first,
            content_hash(&root).unwrap(),
            "a rename with identical bytes must change the hash"
        );
    }

    #[test]
    fn a_missing_pack_root_hashes_without_error() {
        let temp = TempDir::new().unwrap();
        assert!(content_hash(&temp.path().join("absent")).is_ok());
    }

    #[test]
    fn canonical_hashes_are_lowercase_sha256() {
        let hash = sha256_bytes(b"abc");
        assert_eq!(
            hash,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(is_canonical_sha256(&hash));
        assert!(!is_canonical_sha256(&hash.to_uppercase()));
        assert!(!is_canonical_sha256("sha256:abc"));
    }

    #[test]
    fn item_hashing_distinguishes_missing_file_and_tree() {
        let temp = TempDir::new().unwrap();
        let missing = temp.path().join("missing");
        assert_eq!(hash_file(&missing).unwrap(), ContentHash::Missing);
        assert_eq!(hash_tree(&missing).unwrap(), ContentHash::Missing);

        std::fs::create_dir(&missing).unwrap();
        let ContentHash::Present(empty_tree) = hash_tree(&missing).unwrap() else {
            panic!("an existing empty tree must have a hash")
        };
        assert!(is_canonical_sha256(&empty_tree));
    }

    #[test]
    fn tree_hashing_is_order_independent_and_path_sensitive() {
        let left = TempDir::new().unwrap();
        let right = TempDir::new().unwrap();
        write(&left.path().join("a/one.md"), "one");
        write(&left.path().join("b/two.md"), "two");
        write(&right.path().join("b/two.md"), "two");
        write(&right.path().join("a/one.md"), "one");
        assert_eq!(
            hash_tree(left.path()).unwrap(),
            hash_tree(right.path()).unwrap()
        );

        std::fs::rename(
            right.path().join("a/one.md"),
            right.path().join("a/renamed.md"),
        )
        .unwrap();
        assert_ne!(
            hash_tree(left.path()).unwrap(),
            hash_tree(right.path()).unwrap()
        );
    }

    #[test]
    fn mcp_hashing_canonicalizes_nested_map_order() {
        let dir = TempDir::new().unwrap();
        let first_path = dir.path().join("first.yaml");
        let second_path = dir.path().join("second.yaml");
        write(
            &first_path,
            "mcpServers:\n  test:\n    command: test\n    env: {B: two, A: one}\n",
        );
        write(
            &second_path,
            "mcpServers:\n  test:\n    env: {A: one, B: two}\n    command: test\n",
        );
        let first = mcp::parse_pack_mcp_file(&first_path)
            .unwrap()
            .remove("test")
            .unwrap();
        let second = mcp::parse_pack_mcp_file(&second_path)
            .unwrap()
            .remove("test")
            .unwrap();
        assert_eq!(
            hash_mcp_definition(&first).unwrap(),
            hash_mcp_definition(&second).unwrap()
        );
    }
}
