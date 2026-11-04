//! Staging: turning one entry's origin tree into a flat pack root.
//!
//! The same function stages for CI's derivation and for the app's install, so
//! the two produce byte-identical roots and therefore identical hashes. A
//! [`StagePlan::Tree`] copies the origin directory as it is, which is how a
//! connector moved out of the app bundle keeps exactly the hash it had there. A
//! [`StagePlan::Select`] copies listed paths and writes a native manifest built
//! from the entry's metadata. Symbolic links anywhere are refused.

use std::path::{Path, PathBuf};

use serde::Serialize;

use super::derive::GitFetch;
use super::index::{EntryOrigin, IndexEntry};
use super::source::LibrarySource;
use super::verify::{origin_description, verify_staged, VerifiedIndex};
use crate::lock::{PackSource, UpstreamRef};
use crate::manifest::{PackAuthor, PackManifest, MANIFEST_FILE};

#[derive(Debug, Clone, PartialEq)]
pub enum StagePlan {
    /// Copy the origin directory as it is (without `.git`).
    Tree,
    /// Copy each `(from, to)` and write `manifest` as `cairn-pack.yaml`.
    Select {
        files: Vec<(String, String)>,
        manifest: Box<SynthesizedManifest>,
    },
}

/// The native manifest a selection entry is given. Field order is fixed by the
/// struct, so the same metadata always yields the same bytes.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SynthesizedManifest {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<PackAuthor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub keywords: Vec<String>,
}

/// The plan that reproduces the root an index entry was derived from.
pub fn plan_for_entry(entry: &IndexEntry) -> StagePlan {
    if !entry.is_selection() {
        return StagePlan::Tree;
    }
    StagePlan::Select {
        files: entry
            .items
            .iter()
            .filter_map(|item| Some((item.from.clone()?, item.path.clone()?)))
            .collect(),
        manifest: Box::new(SynthesizedManifest {
            id: entry.id.clone(),
            name: entry.name.clone(),
            version: entry.version.clone(),
            description: entry.description.clone(),
            author: entry.author.clone(),
            homepage: entry.homepage.clone(),
            license: entry.license.clone(),
            keywords: entry.keywords.clone(),
        }),
    }
}

/// Build the pack root `dest` (which must not exist) from `origin_root`.
pub fn stage(plan: &StagePlan, origin_root: &Path, dest: &Path) -> Result<(), String> {
    if dest.exists() {
        return Err(format!("refusing to stage into existing {dest:?}"));
    }
    reject_links_on_path(origin_root.parent().unwrap_or(origin_root), origin_root)?;
    if !origin_root.is_dir() {
        return Err(format!("{} is not a directory", origin_root.display()));
    }
    match plan {
        // A bare `SKILL.md` root is one skill; it lands in the conventional
        // layout the install sync copies from, named after the entry.
        StagePlan::Tree if is_bare_skill(origin_root) => {
            let name = dest
                .file_name()
                .ok_or_else(|| format!("{dest:?} has no name"))?;
            copy_tree(origin_root, &dest.join("skills").join(name), "")
        }
        StagePlan::Tree => copy_tree(origin_root, dest, ""),
        StagePlan::Select { files, manifest } => {
            std::fs::create_dir_all(dest).map_err(|e| format!("Failed to create {dest:?}: {e}"))?;
            for (from, to) in files {
                let source = origin_root.join(from);
                reject_links_on_path(origin_root, &source)?;
                let target = dest.join(to);
                if target.exists() {
                    return Err(format!("two listed paths both install to `{to}`"));
                }
                let metadata = std::fs::symlink_metadata(&source)
                    .map_err(|_| format!("listed path `{from}` does not exist"))?;
                if metadata.is_dir() {
                    copy_tree(&source, &target, from)?;
                } else {
                    copy_file(&source, &target)?;
                }
            }
            let yaml = serde_yaml::to_string(manifest)
                .map_err(|e| format!("Failed to write a manifest for `{}`: {e}", manifest.id))?;
            std::fs::write(dest.join(MANIFEST_FILE), yaml)
                .map_err(|e| format!("Failed to write {MANIFEST_FILE}: {e}"))
        }
    }
}

/// A pack root staged from a verified index and checked against it, ready to
/// hand to the ordinary install machinery.
#[derive(Debug, Clone, PartialEq)]
pub struct StagedEntry {
    /// Rooted at the staged directory.
    pub manifest: PackManifest,
    /// The provenance the install lock records.
    pub source: PackSource,
}

/// Fetch the origin of `entry_id` at the commit the index pins, stage it under
/// `stage_parent`, and verify every byte against the index.
pub fn stage_verified_entry(
    library: &LibrarySource,
    verified: &VerifiedIndex,
    entry_id: &str,
    stage_parent: &Path,
    fetch: &dyn GitFetch,
) -> Result<StagedEntry, String> {
    let index = &verified.index;
    let entry = verified
        .entry(entry_id)
        .ok_or_else(|| format!("the {} does not offer `{entry_id}`", library.label))?;
    let (url, commit, path, upstream) = match &entry.origin {
        EntryOrigin::Source { path } => (&index.source.url, &index.source.commit, path, None),
        EntryOrigin::Upstream { url, commit, path } => (
            url,
            commit,
            path,
            Some(UpstreamRef {
                url: url.clone(),
                sha: commit.clone(),
            }),
        ),
    };
    let checkout = stage_parent.join("checkout");
    fetch.fetch(url, commit, &checkout)?;
    let dest = stage_parent.join("pack").join(&entry.id);
    let origin = origin_description(&index.source, entry);
    stage(&plan_for_entry(entry), &checkout.join(path), &dest)
        .map_err(|e| format!("pack `{}` from {origin} could not be staged: {e}", entry.id))?;
    let manifest = verify_staged(entry, &dest, &origin)?;
    Ok(StagedEntry {
        source: PackSource::library(
            library.url.clone(),
            library.git_ref.clone(),
            index.source.commit.clone(),
            verified.signer.clone(),
            upstream,
            manifest.format,
        ),
        manifest,
    })
}

fn is_bare_skill(root: &Path) -> bool {
    root.join("SKILL.md").is_file()
        && !root.join(MANIFEST_FILE).is_file()
        && !root.join(crate::manifest::CLAUDE_PLUGIN_MANIFEST).is_file()
}

fn reject_links_on_path(root: &Path, path: &Path) -> Result<(), String> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let mut current = PathBuf::from(root);
    for component in relative.components() {
        current.push(component);
        if std::fs::symlink_metadata(&current).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(link_refusal(&current));
        }
    }
    Ok(())
}

fn link_refusal(path: &Path) -> String {
    format!(
        "{} is a symbolic link; library content may not contain links",
        path.display()
    )
}

fn copy_tree(source: &Path, dest: &Path, label: &str) -> Result<(), String> {
    std::fs::create_dir_all(dest).map_err(|e| format!("Failed to create {dest:?}: {e}"))?;
    let mut entries: Vec<_> = std::fs::read_dir(source)
        .map_err(|e| format!("Failed to read {source:?}: {e}"))?
        .filter_map(Result::ok)
        .collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| format!("Failed to stat {path:?}: {e}"))?;
        let target = dest.join(&name);
        if file_type.is_symlink() {
            let shown = Path::new(label).join(&name);
            return Err(link_refusal(&shown));
        } else if file_type.is_dir() {
            let child = Path::new(label).join(&name);
            copy_tree(&path, &target, &child.to_string_lossy())?;
        } else if file_type.is_file() {
            copy_file(&path, &target)?;
        } else {
            return Err(format!("{} is not a regular file", path.display()));
        }
    }
    Ok(())
}

fn copy_file(source: &Path, dest: &Path) -> Result<(), String> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("Failed to create {parent:?}: {e}"))?;
    }
    std::fs::copy(source, dest)
        .map(|_| ())
        .map_err(|e| format!("Failed to copy {source:?}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn a_tree_copy_preserves_bytes_and_drops_git() {
        let temp = tempfile::tempdir().unwrap();
        let origin = temp.path().join("linear");
        write(&origin.join("cairn-pack.yaml"), "id: linear\n");
        write(&origin.join("mcp.yaml"), "mcpServers: {}\n");
        write(&origin.join(".git/HEAD"), "ref\n");
        let dest = temp.path().join("stage/linear");
        stage(&StagePlan::Tree, &origin, &dest).unwrap();
        assert_eq!(
            crate::content_hash(&dest).unwrap(),
            {
                std::fs::remove_dir_all(origin.join(".git")).unwrap();
                crate::content_hash(&origin).unwrap()
            },
            "a staged connector hashes exactly as its source does"
        );
        assert!(!dest.join(".git").exists());
    }

    #[test]
    fn a_bare_skill_root_stages_as_one_conventional_skill() {
        let temp = tempfile::tempdir().unwrap();
        let origin = temp.path().join("repo");
        write(&origin.join("SKILL.md"), "---\nname: solo\n---\n");
        write(&origin.join("scripts/run.py"), "print()\n");
        let dest = temp.path().join("stage/solo");
        stage(&StagePlan::Tree, &origin, &dest).unwrap();
        let manifest = crate::manifest::load(&dest).unwrap();
        assert_eq!(manifest.id, "solo");
        assert_eq!(
            manifest.items(),
            vec![crate::PackItem::file(
                crate::PackItemKind::Skill,
                "solo",
                "skills/solo"
            )]
        );
        assert!(dest.join("skills/solo/scripts/run.py").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn links_are_refused_wherever_they_are() {
        let temp = tempfile::tempdir().unwrap();
        let origin = temp.path().join("repo");
        write(&origin.join("skills/pdf/SKILL.md"), "pdf\n");
        std::os::unix::fs::symlink("/etc/passwd", origin.join("skills/pdf/leak")).unwrap();
        let error = stage(&StagePlan::Tree, &origin, &temp.path().join("a")).unwrap_err();
        assert!(error.contains("symbolic link"), "{error}");

        std::fs::remove_file(origin.join("skills/pdf/leak")).unwrap();
        std::os::unix::fs::symlink(origin.join("skills/pdf"), origin.join("skills/alias")).unwrap();
        let plan = StagePlan::Select {
            files: vec![("skills/alias".into(), "skills/alias".into())],
            manifest: Box::new(SynthesizedManifest {
                id: "x".into(),
                name: "x".into(),
                version: "0.0.0".into(),
                description: String::new(),
                author: None,
                homepage: None,
                license: None,
                keywords: vec![],
            }),
        };
        let error = stage(&plan, &origin, &temp.path().join("b")).unwrap_err();
        assert!(error.contains("symbolic link"), "{error}");
    }
}
