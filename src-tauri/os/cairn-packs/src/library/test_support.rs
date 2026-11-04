//! A minisign-format signer for fixture indexes.
//!
//! Produces exactly what `minisign -S` writes (prehashed Ed25519 over a
//! BLAKE2b-512 digest, plus the global signature over the trusted comment), so
//! tests exercise the production verifier without the production key.

use base64::Engine;
use blake2::Digest;
use ring::signature::{Ed25519KeyPair, KeyPair};

use super::source::{LibrarySource, SourceTrust, TrustedKey};

pub struct TestSigner {
    id: String,
    key_id: [u8; 8],
    pair: Ed25519KeyPair,
}

impl TestSigner {
    /// A deterministic key: the same `seed` always yields the same key.
    pub fn new(id: &str, seed: u8) -> Self {
        let pair = Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).expect("valid seed");
        TestSigner {
            id: id.to_string(),
            key_id: [seed; 8],
            pair,
        }
    }

    pub fn trusted_key(&self) -> TrustedKey {
        let mut bin = b"Ed".to_vec();
        bin.extend_from_slice(&self.key_id);
        bin.extend_from_slice(self.pair.public_key().as_ref());
        TrustedKey {
            id: self.id.clone(),
            minisign_public_key: base64::engine::general_purpose::STANDARD.encode(bin),
        }
    }

    /// The `.minisig` text for `bytes`.
    pub fn sign(&self, bytes: &[u8]) -> String {
        let digest = blake2::Blake2b512::digest(bytes);
        let signature = self.pair.sign(&digest);
        let mut bin = b"ED".to_vec();
        bin.extend_from_slice(&self.key_id);
        bin.extend_from_slice(signature.as_ref());
        let trusted_comment = "cairn-library test fixture";
        let mut global = signature.as_ref().to_vec();
        global.extend_from_slice(trusted_comment.as_bytes());
        let global_signature = self.pair.sign(&global);
        let engine = base64::engine::general_purpose::STANDARD;
        format!(
            "untrusted comment: signature from test key\n{}\ntrusted comment: {trusted_comment}\n{}\n",
            engine.encode(bin),
            engine.encode(global_signature.as_ref())
        )
    }
}

/// A signed-index source at `url` trusting exactly `signer`.
pub fn signed_source(url: &str, signer: &TestSigner) -> LibrarySource {
    LibrarySource {
        id: "test-library".to_string(),
        label: "Test library".to_string(),
        url: url.to_string(),
        git_ref: "main".to_string(),
        trust: SourceTrust::SignedIndex {
            index_ref: "cairn-index".to_string(),
            keys: vec![signer.trusted_key()],
        },
    }
}

/// A throwaway git repository for `file://` fetch tests.
pub struct FixtureRepo {
    pub root: std::path::PathBuf,
}

impl FixtureRepo {
    pub fn new(root: std::path::PathBuf) -> Self {
        std::fs::create_dir_all(&root).unwrap();
        let repo = FixtureRepo { root };
        repo.git(&["init", "-q", "-b", "main"]);
        repo
    }

    pub fn url(&self) -> String {
        url::Url::from_directory_path(&self.root)
            .expect("absolute fixture path")
            .to_string()
            .trim_end_matches('/')
            .to_string()
    }

    pub fn write(&self, relative: &str, content: &str) {
        let path = self.root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    /// Copy a directory tree into the repository at `relative`.
    pub fn copy_dir(&self, from: &std::path::Path, relative: &str) {
        let to = self.root.join(relative);
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            if entry.path().is_dir() {
                self.copy_dir(&entry.path(), &format!("{relative}/{name}"));
            } else {
                std::fs::create_dir_all(&to).unwrap();
                std::fs::copy(entry.path(), to.join(&name)).unwrap();
            }
        }
    }

    /// Commit everything on the current branch; returns the commit.
    pub fn commit(&self, message: &str) -> String {
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "--allow-empty", "-m", message]);
        self.git(&["rev-parse", "HEAD"]).trim().to_string()
    }

    /// Publish `index_json` and its signature on the orphan `cairn-index`
    /// branch, as the library's CI does, leaving `main` checked out.
    pub fn publish_index(&self, index_json: &str, signature: &str) -> String {
        let has_branch = !self
            .git(&["branch", "--list", "cairn-index"])
            .trim()
            .is_empty();
        if has_branch {
            self.git(&["checkout", "-q", "cairn-index"]);
        } else {
            self.git(&["checkout", "-q", "--orphan", "cairn-index"]);
            self.git(&["rm", "-rq", "--cached", "."]);
            self.git(&["clean", "-fdq"]);
        }
        self.write("index.json", index_json);
        self.write("index.json.minisig", signature);
        self.git(&["add", "index.json", "index.json.minisig"]);
        self.git(&["commit", "-q", "-m", "index"]);
        let commit = self.git(&["rev-parse", "HEAD"]).trim().to_string();
        self.git(&["checkout", "-q", "-f", "main"]);
        commit
    }

    pub fn git(&self, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&self.root)
            .args([
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(
                "GIT_CONFIG_GLOBAL",
                if cfg!(windows) { "NUL" } else { "/dev/null" },
            )
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).to_string()
    }
}

pub const FIXTURE_LIBRARY_URL: &str = "https://github.com/bleugreenlab/cairn-library";
pub const FIXTURE_LIBRARY_COMMIT: &str = "1111111111111111111111111111111111111111";
pub const FIXTURE_UPSTREAM_URL: &str = "https://github.com/anthropics/skills";
pub const FIXTURE_UPSTREAM_COMMIT: &str = "ca45fc4000000000000000000000000000000000";

fn write_file(root: &std::path::Path, relative: &str, content: &str) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// A library repository in marketplace layout covering every derivation case:
/// a native pack, a Claude Code plugin, a root selection, a SHA-pinned
/// upstream selection, an unpinned remote, and an npm source.
pub fn write_fixture_library(root: &std::path::Path) {
    let marketplace = format!(
        r#"{{
  "name": "cairn-library",
  "owner": {{ "name": "Cairn" }},
  "plugins": [
    {{ "name": "linear", "source": "./plugins/linear" }},
    {{ "name": "acme-tools", "source": "./plugins/acme-tools" }},
    {{ "name": "local-skills", "source": "./", "strict": false,
      "skills": ["./shared/skills/notes"], "version": "1.0.0",
      "description": "Skills selected from the repository root" }},
    {{ "name": "document-skills",
      "source": {{ "source": "github", "repo": "anthropics/skills", "sha": "{FIXTURE_UPSTREAM_COMMIT}" }},
      "strict": false, "skills": ["./skills/pdf", "./skills/docx"],
      "description": "Document skills", "license": "Proprietary (source-available)" }},
    {{ "name": "floating", "source": {{ "source": "github", "repo": "example/floating" }} }},
    {{ "name": "from-npm", "source": {{ "source": "npm", "package": "@example/plugin" }} }}
  ]
}}
"#
    );
    write_file(root, ".claude-plugin/marketplace.json", &marketplace);
    write_file(
        root,
        "plugins/linear/cairn-pack.yaml",
        "id: linear\nname: Linear\nversion: 1.0.0\ndescription: Linear issues.\n",
    );
    write_file(
        root,
        "plugins/linear/mcp.yaml",
        "mcpServers:\n  linear:\n    type: http\n    url: https://mcp.linear.app/mcp\n    oauth: {}\n",
    );
    write_file(
        root,
        "plugins/acme-tools/.claude-plugin/plugin.json",
        r#"{"name":"acme-tools","displayName":"Acme Tools","version":"2.0.0","author":"Acme"}"#,
    );
    write_file(
        root,
        "plugins/acme-tools/skills/acme/SKILL.md",
        "---\nname: acme\ndescription: Acme\n---\n",
    );
    write_file(
        root,
        "plugins/acme-tools/.mcp.json",
        r#"{"mcpServers":{"acme":{"command":"acme-mcp","args":["--stdio"],"env":{"B":"2","A":"1"}}}}"#,
    );
    write_file(root, "plugins/acme-tools/commands/run.md", "run\n");
    write_file(
        root,
        "shared/skills/notes/SKILL.md",
        "---\nname: notes\ndescription: Notes\n---\n",
    );
    write_file(root, "README.md", "The fixture library.\n");
}

/// The upstream repository `document-skills` selects from.
pub fn write_fixture_upstream(root: &std::path::Path) {
    write_file(
        root,
        "skills/pdf/SKILL.md",
        "---\nname: pdf\ndescription: PDF\n---\n",
    );
    write_file(root, "skills/pdf/scripts/fill.py", "print('fill')\n");
    write_file(
        root,
        "skills/docx/SKILL.md",
        "---\nname: docx\ndescription: DOCX\n---\n",
    );
    write_file(
        root,
        "skills/unlisted/SKILL.md",
        "---\nname: unlisted\n---\n",
    );
    write_file(root, "README.md", "Upstream.\n");
}

/// A [`super::derive::GitFetch`] that copies a local directory per URL and
/// reports the requested commit, for derivation tests with stable URLs.
pub struct DirFetch(pub std::collections::BTreeMap<String, std::path::PathBuf>);

impl super::derive::GitFetch for DirFetch {
    fn fetch(&self, url: &str, commit: &str, dest: &std::path::Path) -> Result<String, String> {
        let from = self
            .0
            .get(url)
            .ok_or_else(|| format!("fixture has no repository {url}"))?;
        super::stage::stage(&super::stage::StagePlan::Tree, from, dest)?;
        Ok(commit.to_string())
    }
}
