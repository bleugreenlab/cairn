//! A fixture Cairn library: a real git repository in marketplace layout whose
//! signed index is published on `cairn-index`, fetched over `file://`.

use std::path::{Path, PathBuf};

use crate::config::pack::library::derive::{derive_index, PolicyFetch};
use crate::config::pack::library::git::GitPolicy;
use crate::config::pack::library::source::CAIRN_LIBRARY_ID;
use crate::config::pack::library::test_support::{signed_source, FixtureRepo, TestSigner};
use crate::config::pack::library::{IndexSource, LibrarySource};

use super::LibraryContext;

pub struct LibraryFixture {
    pub repo: FixtureRepo,
    signer: TestSigner,
    serial: u64,
    _temp: tempfile::TempDir,
}

impl Default for LibraryFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl LibraryFixture {
    pub fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let repo = FixtureRepo::new(temp.path().join("cairn-library"));
        LibraryFixture {
            repo,
            signer: TestSigner::new("cairn-library-test", 11),
            serial: 0,
            _temp: temp,
        }
    }

    /// Write `content` at `plugins/<pack>/<relative>`.
    pub fn write(&self, pack: &str, relative: &str, content: &str) {
        self.repo
            .write(&format!("plugins/{pack}/{relative}"), content);
    }

    pub fn remove(&self, pack: &str, relative: &str) {
        let path = self.repo.root.join("plugins").join(pack).join(relative);
        if path.is_dir() {
            std::fs::remove_dir_all(path).unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
    }

    /// Commit `main`, derive its index, sign it, and publish it on
    /// `cairn-index`, as the library's CI does. Returns the index bytes.
    pub fn publish(&mut self) -> String {
        let plugins: Vec<String> = std::fs::read_dir(self.repo.root.join("plugins"))
            .map(|entries| {
                let mut names: Vec<String> = entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.file_name().to_string_lossy().to_string())
                    .collect();
                names.sort();
                names
            })
            .unwrap_or_default();
        let listed: Vec<String> = plugins
            .iter()
            .map(|name| format!(r#"{{"name":"{name}","source":"./plugins/{name}"}}"#))
            .collect();
        self.repo.write(
            ".claude-plugin/marketplace.json",
            &format!(
                r#"{{"name":"cairn-library","plugins":[{}]}}"#,
                listed.join(",")
            ),
        );
        let commit = self.repo.commit("library");
        self.serial += 1;
        let index = derive_index(
            &self.repo.root,
            IndexSource {
                url: self.repo.url(),
                git_ref: "main".into(),
                commit,
            },
            self.serial,
            "2026-11-02T00:00:00Z".into(),
            &PolicyFetch(GitPolicy::allowing_file_urls_for_tests()),
        )
        .unwrap();
        let bytes = index.to_canonical_json().unwrap();
        self.repo
            .publish_index(&bytes, &self.signer.sign(bytes.as_bytes()));
        bytes
    }

    /// Publish exactly `bytes`, correctly signed (a forged-but-signed index).
    pub fn publish_signed(&self, bytes: &str) {
        self.repo
            .publish_index(bytes, &self.signer.sign(bytes.as_bytes()));
    }

    /// Publish `bytes` with a signature that does not match them.
    pub fn publish_tampered(&self, bytes: &str) {
        let signature = self.signer.sign(b"something else");
        self.repo.publish_index(bytes, &signature);
    }

    /// The Cairn library, pointed at this fixture.
    pub fn source(&self) -> LibrarySource {
        let mut source = signed_source(&self.repo.url(), &self.signer);
        source.id = CAIRN_LIBRARY_ID.to_string();
        source.label = "Cairn library".to_string();
        source
    }

    pub fn context(&self) -> LibraryContext {
        LibraryContext {
            sources: vec![self.source()],
            policy: GitPolicy::allowing_file_urls_for_tests(),
        }
    }

    pub fn root(&self) -> &Path {
        &self.repo.root
    }

    pub fn cache_dir(&self, config_dir: &Path) -> PathBuf {
        crate::config::pack::library::cache::cache_dir(config_dir, &self.source())
    }
}
