use std::path::PathBuf;

use cairn_codec::objects::{ObjectReadError, ObjectReadLimits, ObjectStore, TreeEntry};

const SERVE_MAX_DEPTH: usize = 128;
const SERVE_MAX_ENTRIES: usize = 100_000;
const SERVE_MAX_BLOB_BYTES: usize = 32 * 1024 * 1024;

pub(super) struct ObjectReadService {
    commit_id: String,
    prefix: String,
    store: ObjectStore,
    limits: ObjectReadLimits,
}

impl ObjectReadService {
    pub fn new(
        repository_path: PathBuf,
        commit_id: String,
        prefix: String,
    ) -> Result<Self, String> {
        let store = ObjectStore::new(&repository_path, None)
            .map_err(|error| format!("open repository object store: {error}"))?;
        let limits = ObjectReadLimits::new(
            SERVE_MAX_DEPTH,
            SERVE_MAX_ENTRIES,
            SERVE_MAX_BLOB_BYTES,
            std::time::Duration::from_secs(30),
        );
        Ok(Self {
            commit_id,
            prefix,
            store,
            limits,
        })
    }

    pub fn listing(&self) -> Result<Vec<(String, bool, u64)>, ObjectReadError> {
        self.entries()?
            .into_iter()
            .map(|entry| {
                let is_dir = entry.kind == cairn_codec::objects::TreeEntryKind::Tree;
                let size = if is_dir {
                    0
                } else {
                    self.store.blob(&entry.oid, &self.limits)?.len() as u64
                };
                Ok((entry.name, is_dir, size))
            })
            .collect()
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn commit_id(&self) -> &str {
        &self.commit_id
    }

    pub fn limits(&self) -> &ObjectReadLimits {
        &self.limits
    }

    pub fn bytes(&self) -> Result<Vec<u8>, ObjectReadError> {
        self.store
            .read_path_at_commit(&self.commit_id, &self.prefix, &self.limits)
    }

    pub fn entries(&self) -> Result<Vec<TreeEntry>, ObjectReadError> {
        self.store
            .entries_at_commit(&self.commit_id, &self.prefix, &self.limits)
    }

    pub fn files(&self) -> Result<Vec<(String, Vec<u8>)>, ObjectReadError> {
        let entries = self
            .store
            .walk_commit(&self.commit_id, "", &self.limits)?
            .into_iter()
            .filter(|item| item.kind == cairn_codec::objects::TreeEntryKind::Blob)
            .map(|item| ContentEntry {
                path: item.path,
                oid: item.oid,
                mode: item.mode,
            })
            .collect();
        visible_files(entries, &self.prefix, |oid| {
            self.store
                .blob(oid, &self.limits)
                .map(|bytes| std::sync::Arc::from(bytes.into_boxed_slice()))
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ContentEntry {
    pub path: String,
    pub oid: String,
    pub mode: u16,
}

pub(super) fn visible_files(
    entries: Vec<ContentEntry>,
    prefix: &str,
    mut load: impl FnMut(&str) -> Result<std::sync::Arc<[u8]>, ObjectReadError>,
) -> Result<Vec<(String, Vec<u8>)>, ObjectReadError> {
    let entries = visible_entries(entries, prefix, &mut load)?;
    entries
        .into_iter()
        .map(|entry| {
            let bytes = load(&entry.oid)?;
            Ok((entry.path, bytes.to_vec()))
        })
        .collect()
}

pub(super) fn visible_entries(
    entries: Vec<ContentEntry>,
    prefix: &str,
    mut load_ignore: impl FnMut(&str) -> Result<std::sync::Arc<[u8]>, ObjectReadError>,
) -> Result<Vec<ContentEntry>, ObjectReadError> {
    const SYMLINK_MODE: u16 = 0o120000;

    let mut ignore_files = Vec::new();
    for entry in &entries {
        if entry.mode != SYMLINK_MODE
            && std::path::Path::new(&entry.path)
                .file_name()
                .is_some_and(|name| name == ".gitignore")
        {
            ignore_files.push((entry.path.clone(), load_ignore(&entry.oid)?));
        }
    }

    let mut ignores = Vec::new();
    for (path, bytes) in ignore_files {
        let directory = std::path::Path::new(&path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new(""))
            .to_path_buf();
        let mut builder = ignore::gitignore::GitignoreBuilder::new(&directory);
        for line in String::from_utf8_lossy(&bytes).lines() {
            builder
                .add_line(Some(std::path::PathBuf::from(&path)), line)
                .map_err(|error| ObjectReadError::InvalidPath(error.to_string()))?;
        }
        let matcher = builder
            .build()
            .map_err(|error| ObjectReadError::InvalidPath(error.to_string()))?;
        ignores.push((directory, matcher));
    }
    ignores.sort_by_key(|(directory, _)| directory.components().count());

    let prefix = prefix.trim_matches('/');
    let mut files = Vec::new();
    for entry in entries {
        if entry.mode == SYMLINK_MODE
            || (!prefix.is_empty()
                && entry.path != prefix
                && !entry.path.starts_with(&format!("{prefix}/")))
        {
            continue;
        }
        let logical = std::path::Path::new(&entry.path);
        let mut ignored = false;
        for (directory, matcher) in &ignores {
            if logical.starts_with(directory) {
                let matched = matcher.matched_path_or_any_parents(logical, false);
                if matched.is_ignore() {
                    ignored = true;
                } else if matched.is_whitelist() {
                    ignored = false;
                }
            }
        }
        if ignored {
            continue;
        }
        let path = entry
            .path
            .strip_prefix(prefix)
            .and_then(|path| path.strip_prefix('/'))
            .unwrap_or(&entry.path)
            .to_string();
        files.push(ContentEntry {
            path,
            oid: entry.oid,
            mode: entry.mode,
        });
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_codec::testutil::{commit_all, git, init_repo, write_file};

    #[test]
    fn visible_files_use_gitignore_blobs_from_the_selected_commit() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, ".gitignore", b"ignored/*\n!ignored/keep.txt\n");
        write_file(repo, "ignored/drop.txt", b"drop");
        write_file(repo, "ignored/keep.txt", b"keep");
        write_file(repo, "nested/.gitignore", b"*.log\n!keep.log\n");
        write_file(repo, "nested/drop.log", b"drop");
        write_file(repo, "nested/keep.log", b"keep");
        write_file(repo, "visible.txt", b"visible");
        git(repo, &["add", "-f", "."]);
        let commit = commit_all(repo, "ignored fixture");

        let service = ObjectReadService::new(repo.to_path_buf(), commit, String::new()).unwrap();
        let paths: Vec<_> = service
            .files()
            .unwrap()
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        assert!(paths.contains(&".gitignore".to_string()));
        assert!(paths.contains(&"ignored/keep.txt".to_string()));
        assert!(paths.contains(&"nested/keep.log".to_string()));
        assert!(paths.contains(&"visible.txt".to_string()));
        assert!(!paths.contains(&"ignored/drop.txt".to_string()));
        assert!(!paths.contains(&"nested/drop.log".to_string()));
    }
}
