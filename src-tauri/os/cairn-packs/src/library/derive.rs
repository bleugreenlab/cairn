//! Deriving an index from a source tree.
//!
//! One implementation, run by the library repository's CI (through the
//! `cairn-library-index` binary) and by this crate's tests, so the hashes CI
//! signs and the hashes the app checks cannot drift: both come from
//! [`entry_from_staged`], which reads a staged root with the lock's own hash
//! functions.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::index::{EntryOrigin, IndexEntry, IndexItem, IndexSource, LibraryIndex, INDEX_FORMAT};
use super::marketplace::{self, PluginSource, PluginSpec, MARKETPLACE_FILE};
use super::stage::{stage, StagePlan, SynthesizedManifest};
use super::{is_full_commit, is_safe_relative};
use crate::manifest::{self, normalize_id, ItemRuns, PackItemKind};
use crate::server_config::McpServerConfig;
use crate::ContentHash;

/// Fetches another repository at a pinned commit.
pub trait GitFetch {
    /// Fetch `commit` of `url` into the new directory `dest`; returns the
    /// resolved commit.
    fn fetch(&self, url: &str, commit: &str, dest: &Path) -> Result<String, String>;
}

/// [`GitFetch`] through [`super::git::fetch_tree`] under a policy.
pub struct PolicyFetch(pub super::git::GitPolicy);

impl GitFetch for PolicyFetch {
    fn fetch(&self, url: &str, commit: &str, dest: &Path) -> Result<String, String> {
        super::git::fetch_tree(url, commit, dest, self.0)
    }
}

/// Derive the index for the source tree checked out at `root`.
pub fn derive_index(
    root: &Path,
    source: IndexSource,
    serial: u64,
    generated_at: String,
    fetch: &dyn GitFetch,
) -> Result<LibraryIndex, String> {
    if !is_full_commit(&source.commit) {
        return Err(format!(
            "commit `{}` is not a full 40-hex commit",
            source.commit
        ));
    }
    let work = tempfile::tempdir().map_err(|e| e.to_string())?;
    let mut entries = Vec::new();
    let mut notes = Vec::new();

    let marketplace_path = root.join(MARKETPLACE_FILE);
    if marketplace_path.is_file() {
        let text = std::fs::read_to_string(&marketplace_path)
            .map_err(|e| format!("Failed to read {MARKETPLACE_FILE}: {e}"))?;
        let (specs, skipped) = marketplace::parse(&text)?;
        notes.extend(skipped);
        let mut fetched: BTreeMap<(String, String), PathBuf> = BTreeMap::new();
        for (index, spec) in specs.iter().enumerate() {
            let stage_root = work.path().join(format!("stage-{index}"));
            match derive_entry(spec, root, &stage_root, &mut fetched, work.path(), fetch) {
                Ok(entry) => entries.push(entry),
                Err(reason) => notes.push(format!("plugin `{}` skipped: {reason}", spec.name)),
            }
        }
    } else {
        // A source with no marketplace is one pack at its root. Its id falls
        // back to the repository name, never to wherever CI checked it out.
        let id = source
            .url
            .trim_end_matches('/')
            .trim_end_matches(".git")
            .rsplit('/')
            .next()
            .map(normalize_id)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| format!("cannot name a pack after `{}`", source.url))?;
        let dest = work.path().join("root").join(&id);
        stage(&StagePlan::Tree, root, &dest)?;
        let manifest = manifest::load(&dest)?;
        let origin = EntryOrigin::Source {
            path: String::new(),
        };
        entries.push(entry_from_staged(
            &manifest.id,
            origin,
            &dest,
            &BTreeMap::new(),
            Vec::new(),
        )?);
    }

    entries.sort_by(|a, b| a.id.cmp(&b.id));
    let mut ids = BTreeSet::new();
    for entry in &entries {
        if !ids.insert(entry.id.clone()) {
            return Err(format!("two plugins derive to the same id `{}`", entry.id));
        }
    }
    Ok(LibraryIndex {
        format: INDEX_FORMAT.to_string(),
        source,
        serial,
        generated_at,
        entries,
        notes,
    })
}

fn derive_entry(
    spec: &PluginSpec,
    root: &Path,
    stage_root: &Path,
    fetched: &mut BTreeMap<(String, String), PathBuf>,
    work: &Path,
    fetch: &dyn GitFetch,
) -> Result<IndexEntry, String> {
    let id = normalize_id(&spec.name);
    if id.is_empty() {
        return Err("its name is not a usable id".into());
    }
    let (origin, origin_root) = match &spec.source {
        PluginSource::Relative(path) => {
            (EntryOrigin::Source { path: path.clone() }, root.join(path))
        }
        PluginSource::Git { url, commit, path } => {
            let key = (url.clone(), commit.clone());
            let checkout = match fetched.get(&key) {
                Some(checkout) => checkout.clone(),
                None => {
                    let dest = work.join(format!("upstream-{}", fetched.len()));
                    fetch.fetch(url, commit, &dest)?;
                    fetched.insert(key, dest.clone());
                    dest
                }
            };
            (
                EntryOrigin::Upstream {
                    url: url.clone(),
                    commit: commit.clone(),
                    path: path.clone(),
                },
                checkout.join(path),
            )
        }
    };

    let dest = stage_root.join(&id);
    let mut from_paths = BTreeMap::new();
    let plan = if spec.is_selection() {
        let mut files = Vec::new();
        for from in spec.skills.iter().flatten() {
            let name = from.rsplit('/').next().unwrap_or(from);
            files.push((from.clone(), format!("skills/{name}")));
        }
        for from in spec.agents.iter().flatten() {
            let name = from.rsplit('/').next().unwrap_or(from);
            files.push((from.clone(), format!("agents/{name}")));
        }
        for (from, to) in &files {
            if !is_safe_relative(to) {
                return Err(format!("listed path `{from}` has no usable name"));
            }
            from_paths.insert(to.clone(), from.clone());
        }
        StagePlan::Select {
            files,
            manifest: Box::new(SynthesizedManifest {
                id: id.clone(),
                name: spec.name.clone(),
                version: spec.version.clone().unwrap_or_else(|| "0.0.0".into()),
                description: spec.description.clone().unwrap_or_default(),
                author: spec.author.clone(),
                homepage: spec.homepage.clone(),
                license: spec.license.clone(),
                keywords: spec.keywords.clone(),
            }),
        }
    } else {
        StagePlan::Tree
    };
    stage(&plan, &origin_root, &dest)?;
    let entry = entry_from_staged(&id, origin, &dest, &from_paths, spec.notes.clone())?;
    // Every listed path must surface as an item, or an install that stages
    // from the index (which knows only items) would build a different root.
    for to in from_paths.keys() {
        if !entry
            .items
            .iter()
            .any(|item| item.path.as_deref() == Some(to))
        {
            return Err(format!(
                "listed path `{}` is not a skill or agent Cairn recognizes",
                from_paths[to]
            ));
        }
    }
    if spec.is_selection() {
        // A license the marketplace states is what the entry carries; a
        // selection has no manifest of its own to say otherwise.
        debug_assert_eq!(entry.license, spec.license);
    }
    Ok(entry)
}

/// Read a staged pack root into an index entry, hashing every item exactly as
/// the install lock does.
pub fn entry_from_staged(
    id: &str,
    origin: EntryOrigin,
    root: &Path,
    from_paths: &BTreeMap<String, String>,
    extra_notes: Vec<String>,
) -> Result<IndexEntry, String> {
    let manifest = manifest::load(root)?;
    if manifest.id != id {
        return Err(format!(
            "its manifest names the pack `{}`, not `{id}`",
            manifest.id
        ));
    }
    let mcp: BTreeMap<String, McpServerConfig> = match manifest.mcp_source() {
        Some(path) => crate::mcp::parse_pack_mcp_file(&path)?
            .servers
            .into_iter()
            .collect(),
        None => BTreeMap::new(),
    };
    let mut items = Vec::new();
    for item in manifest.items() {
        let definition = (item.kind == PackItemKind::Mcp)
            .then(|| mcp.get(&item.id).cloned())
            .flatten();
        let hash = match (&item.path, &definition) {
            (_, Some(definition)) => crate::hash_mcp_definition(definition)?,
            (Some(path), None) => match crate::hash_item_path(item.kind, root, &root.join(path))? {
                ContentHash::Present(hash) => hash,
                ContentHash::Missing => return Err(format!("`{path}` vanished while hashing")),
            },
            (None, None) => return Err(format!("MCP server `{}` has no definition", item.id)),
        };
        items.push(IndexItem {
            runs: ItemRuns::classify(item.kind, definition.as_ref()),
            from: item
                .path
                .as_ref()
                .and_then(|path| from_paths.get(path).cloned()),
            kind: item.kind,
            id: item.id,
            path: item.path,
            hash,
            definition,
        });
    }
    items.sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
    if items.is_empty() {
        return Err("it ships nothing Cairn can install".into());
    }
    let mut notes = manifest.notes.clone();
    notes.extend(extra_notes);
    Ok(IndexEntry {
        id: manifest.id.clone(),
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        description: manifest.description.clone(),
        author: manifest.author.clone(),
        homepage: manifest.homepage.clone(),
        license: manifest.license.clone(),
        keywords: manifest.keywords.clone(),
        format: manifest.format,
        origin,
        content_hash: crate::content_hash(root)?,
        items,
        notes,
    })
}

#[cfg(test)]
mod tests {
    use super::super::index::IndexSource;
    use super::super::stage::stage_verified_entry;
    use super::super::test_support::*;
    use super::super::verify::verify_index;
    use super::*;

    struct Fixture {
        _temp: tempfile::TempDir,
        library: PathBuf,
        upstream: PathBuf,
        fetch: DirFetch,
    }

    fn fixture() -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let library = temp.path().join("cairn-library");
        let upstream = temp.path().join("skills");
        write_fixture_library(&library);
        write_fixture_upstream(&upstream);
        let fetch = DirFetch(BTreeMap::from([
            (FIXTURE_LIBRARY_URL.to_string(), library.clone()),
            (FIXTURE_UPSTREAM_URL.to_string(), upstream.clone()),
        ]));
        Fixture {
            _temp: temp,
            library,
            upstream,
            fetch,
        }
    }

    fn derive(fixture: &Fixture) -> LibraryIndex {
        derive_index(
            &fixture.library,
            IndexSource {
                url: FIXTURE_LIBRARY_URL.into(),
                git_ref: "main".into(),
                commit: FIXTURE_LIBRARY_COMMIT.into(),
            },
            42,
            "2026-11-02T00:00:00Z".into(),
            &fixture.fetch,
        )
        .unwrap()
    }

    /// The bytes CI signs for the fixture marketplace. A change here changes
    /// every hash a published index carries, so it must be deliberate:
    /// regenerate with `CAIRN_UPDATE_GOLDEN=1`.
    #[test]
    fn the_golden_vector_pins_the_derived_index() {
        let actual = derive(&fixture()).to_canonical_json().unwrap();
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("src/library/testdata/golden-index.json");
        if std::env::var_os("CAIRN_UPDATE_GOLDEN").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &actual).unwrap();
        }
        let expected = std::fs::read_to_string(&path).expect("golden vector is checked in");
        assert_eq!(actual, expected);
    }

    #[test]
    fn every_case_derives_to_its_expected_shape() {
        let index = derive(&fixture());
        let ids: Vec<_> = index.entries.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["acme-tools", "document-skills", "linear", "local-skills"]
        );

        let linear = index.entry("linear").unwrap();
        assert_eq!(linear.runs(), ItemRuns::Endpoint);
        assert!(linear.items[0].definition.is_some());

        let acme = index.entry("acme-tools").unwrap();
        assert_eq!(acme.format, crate::PackFormat::ClaudeCode);
        assert_eq!(acme.runs(), ItemRuns::Here, "its stdio server spawns here");
        assert!(acme.notes.iter().any(|n| n.contains("commands")));

        let documents = index.entry("document-skills").unwrap();
        assert_eq!(
            documents.origin,
            EntryOrigin::Upstream {
                url: FIXTURE_UPSTREAM_URL.into(),
                commit: FIXTURE_UPSTREAM_COMMIT.into(),
                path: String::new()
            }
        );
        assert_eq!(
            documents
                .items
                .iter()
                .map(|i| (i.id.as_str(), i.from.as_deref()))
                .collect::<Vec<_>>(),
            vec![("docx", Some("skills/docx")), ("pdf", Some("skills/pdf"))],
            "only the listed skills, never the unlisted one"
        );
        assert_eq!(
            documents.license.as_deref(),
            Some("Proprietary (source-available)")
        );

        assert!(index
            .notes
            .iter()
            .any(|n| n.starts_with("plugin `floating` skipped")));
        assert!(index
            .notes
            .iter()
            .any(|n| n.starts_with("plugin `from-npm` skipped")));
    }

    #[test]
    fn every_entry_stages_back_from_the_index_and_verifies() {
        let fixture = fixture();
        let index = derive(&fixture);
        let signer = TestSigner::new("k", 3);
        let source = signed_source(FIXTURE_LIBRARY_URL, &signer);
        let bytes = index.to_canonical_json().unwrap();
        let verified =
            verify_index(&source, bytes.as_bytes(), &signer.sign(bytes.as_bytes()), 0).unwrap();
        for entry in &verified.index.entries {
            let work = tempfile::tempdir().unwrap();
            let staged =
                stage_verified_entry(&source, &verified, &entry.id, work.path(), &fixture.fetch)
                    .unwrap_or_else(|e| panic!("{}: {e}", entry.id));
            assert_eq!(staged.manifest.id, entry.id);
            assert_eq!(staged.source.signer.as_deref(), Some("k"));
            assert_eq!(
                staged.source.sha.as_deref(),
                Some(FIXTURE_LIBRARY_COMMIT),
                "the lock records the library commit that vouched for it"
            );
            assert_eq!(
                staged.source.upstream.is_some(),
                matches!(entry.origin, EntryOrigin::Upstream { .. })
            );
        }
    }

    fn verified(fixture: &Fixture) -> (super::super::LibrarySource, super::super::VerifiedIndex) {
        let index = derive(fixture);
        let signer = TestSigner::new("k", 3);
        let source = signed_source(FIXTURE_LIBRARY_URL, &signer);
        let bytes = index.to_canonical_json().unwrap();
        let verified =
            verify_index(&source, bytes.as_bytes(), &signer.sign(bytes.as_bytes()), 0).unwrap();
        (source, verified)
    }

    #[test]
    fn content_that_differs_from_the_index_is_refused_naming_item_and_hashes() {
        let fixture = fixture();
        let (source, verified) = verified(&fixture);
        std::fs::write(
            fixture
                .library
                .join("plugins/acme-tools/skills/acme/SKILL.md"),
            "tampered\n",
        )
        .unwrap();
        let work = tempfile::tempdir().unwrap();
        let error = stage_verified_entry(
            &source,
            &verified,
            "acme-tools",
            work.path(),
            &fixture.fetch,
        )
        .unwrap_err();
        let expected = &verified
            .entry("acme-tools")
            .unwrap()
            .item(PackItemKind::Skill, "acme")
            .unwrap()
            .hash;
        assert!(error.starts_with("skill `acme` from https://github.com/bleugreenlab/cairn-library at 1111111111111111111111111111111111111111 hashes to "), "{error}");
        assert!(
            error.contains(&format!("but the signed index says {expected}")),
            "{error}"
        );
        assert!(error.ends_with("nothing was installed"), "{error}");
    }

    #[test]
    fn an_upstream_tree_that_differs_is_refused_naming_the_upstream() {
        let fixture = fixture();
        let (source, verified) = verified(&fixture);
        std::fs::write(fixture.upstream.join("skills/pdf/SKILL.md"), "changed\n").unwrap();
        let work = tempfile::tempdir().unwrap();
        let error = stage_verified_entry(
            &source,
            &verified,
            "document-skills",
            work.path(),
            &fixture.fetch,
        )
        .unwrap_err();
        assert!(
            error.starts_with(&format!(
                "skill `pdf` from upstream {FIXTURE_UPSTREAM_URL} at {FIXTURE_UPSTREAM_COMMIT} \
                 hashes to "
            )),
            "{error}"
        );
    }

    #[test]
    fn an_item_the_index_does_not_list_is_refused() {
        let fixture = fixture();
        let (source, verified) = verified(&fixture);
        let extra = fixture.library.join("plugins/linear/skills/extra/SKILL.md");
        std::fs::create_dir_all(extra.parent().unwrap()).unwrap();
        std::fs::write(extra, "---\nname: extra\n---\n").unwrap();
        let work = tempfile::tempdir().unwrap();
        let error = stage_verified_entry(&source, &verified, "linear", work.path(), &fixture.fetch)
            .unwrap_err();
        assert!(
            error.contains("contains skill `extra`, which the signed index does not list"),
            "{error}"
        );
    }

    #[test]
    fn an_index_less_source_derives_one_entry_named_after_the_repository() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("checkout-dir-name-is-irrelevant");
        std::fs::create_dir_all(root.join("skills/solo")).unwrap();
        std::fs::write(root.join("skills/solo/SKILL.md"), "---\nname: solo\n---\n").unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        let index = derive_index(
            &root,
            IndexSource {
                url: "https://github.com/someone/solo-pack.git".into(),
                git_ref: "main".into(),
                commit: FIXTURE_LIBRARY_COMMIT.into(),
            },
            1,
            "t".into(),
            &DirFetch(BTreeMap::new()),
        )
        .unwrap();
        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.entries[0].id, "solo-pack");
        assert_eq!(
            index.entries[0].origin,
            EntryOrigin::Source {
                path: String::new()
            }
        );
    }
}
