//! Believing an index, and believing staged bytes.
//!
//! [`verify_index`] decides whether a fetched (or cached) index is the source's
//! own: signature first, then format, repository, serial, and the shape of
//! every entry. [`verify_staged`] decides whether a staged pack root is exactly
//! what that index describes. Every failure is a named refusal; nothing is
//! installed on a refusal.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::index::{EntryOrigin, IndexEntry, IndexSource, LibraryIndex, INDEX_FORMAT};
use super::source::{LibrarySource, SourceTrust, TrustedKey};
use super::{is_full_commit, is_safe_relative};
use crate::manifest::{normalize_id, ItemRuns, PackItemKind};

/// Ids no library entry may take: they ship with the app.
pub const RESERVED_IDS: &[&str] = &["core"];

/// An index whose signature verified, with the entries it is safe to offer.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedIndex {
    pub index: LibraryIndex,
    /// The id of the key that signed it.
    pub signer: String,
    /// Entries the signed index carries that this build will not offer.
    pub refused: Vec<EntryRefusal>,
}

impl VerifiedIndex {
    pub fn entry(&self, id: &str) -> Option<&IndexEntry> {
        self.index.entry(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryRefusal {
    pub id: String,
    pub reason: String,
}

/// Verify `index_bytes` against `signature_text` and the source it claims to be.
///
/// `min_serial` is the newest serial already verified for this source; an older
/// one is a rollback and is refused.
pub fn verify_index(
    source: &LibrarySource,
    index_bytes: &[u8],
    signature_text: &str,
    min_serial: u64,
) -> Result<VerifiedIndex, String> {
    let SourceTrust::SignedIndex { keys, .. } = &source.trust;
    let signer = verify_signature(source, keys, index_bytes, signature_text)?;
    let label = &source.label;

    let index: LibraryIndex = serde_json::from_slice(index_bytes)
        .map_err(|e| format!("the {label} index is signed but malformed: {e}"))?;
    if index.format != INDEX_FORMAT {
        return Err(format!(
            "the {label} index has format `{}`; this build reads `{INDEX_FORMAT}`",
            index.format
        ));
    }
    if !source.is_url(&index.source.url) {
        return Err(format!(
            "the {label} index was published for `{}`, not `{}`; refusing an index replayed \
             from another repository",
            index.source.url, source.url
        ));
    }
    if !is_full_commit(&index.source.commit) {
        return Err(format!(
            "the {label} index names commit `{}`, which is not a full 40-hex commit",
            index.source.commit
        ));
    }
    if index.serial < min_serial {
        return Err(format!(
            "the {label} index serial {} is older than serial {min_serial} already verified; \
             refusing a rollback",
            index.serial
        ));
    }
    let mut seen = BTreeSet::new();
    for entry in &index.entries {
        if !seen.insert(entry.id.as_str()) {
            return Err(format!(
                "the {label} index lists entry `{}` more than once",
                entry.id
            ));
        }
    }

    let mut index = index;
    let mut refused = Vec::new();
    index.entries.retain(|entry| match check_entry(entry) {
        Ok(()) => true,
        Err(reason) => {
            refused.push(EntryRefusal {
                id: entry.id.clone(),
                reason,
            });
            false
        }
    });
    Ok(VerifiedIndex {
        index,
        signer,
        refused,
    })
}

fn verify_signature(
    source: &LibrarySource,
    keys: &[TrustedKey],
    bytes: &[u8],
    signature_text: &str,
) -> Result<String, String> {
    let label = &source.label;
    if keys.is_empty() {
        return Err(format!(
            "the {label} has no verification key compiled into this build, so its index \
             cannot be trusted"
        ));
    }
    let signature = minisign_verify::Signature::decode(signature_text)
        .map_err(|e| format!("the {label} index signature is unreadable: {e}"))?;
    for key in keys {
        let Ok(public) = minisign_verify::PublicKey::from_base64(&key.minisign_public_key) else {
            log::error!("Trusted key `{}` for {label} is not a minisign key", key.id);
            continue;
        };
        // `allow_legacy: false` accepts only prehashed signatures, the default
        // since minisign 0.10.
        if public.verify(bytes, &signature, false).is_ok() {
            return Ok(key.id.clone());
        }
    }
    Err(format!(
        "index signature does not verify against the {label}'s trusted keys"
    ))
}

/// The per-entry shape rules. A signed index is operator-reviewed, but these
/// are what every later step relies on, so they are checked, not assumed.
fn check_entry(entry: &IndexEntry) -> Result<(), String> {
    let id = &entry.id;
    if id.is_empty() || normalize_id(id) != *id {
        return Err(format!("entry id `{id}` is not a kebab-case pack id"));
    }
    if RESERVED_IDS.contains(&id.as_str()) {
        return Err(format!(
            "`{id}` is bundled with the app; a library entry may not replace it"
        ));
    }
    if !crate::is_canonical_sha256(&entry.content_hash) {
        return Err(format!("entry `{id}` has a malformed content hash"));
    }
    match &entry.origin {
        EntryOrigin::Source { path } => {
            if !is_safe_relative(path) {
                return Err(format!("entry `{id}` has an unsafe origin path `{path}`"));
            }
        }
        EntryOrigin::Upstream { url, commit, path } => {
            if !url.starts_with("https://") {
                return Err(format!(
                    "entry `{id}` references a non-https upstream `{url}`"
                ));
            }
            if !is_full_commit(commit) {
                return Err(format!(
                    "entry `{id}` pins upstream `{url}` to `{commit}`, which is not a full \
                     40-hex commit"
                ));
            }
            if !is_safe_relative(path) {
                return Err(format!("entry `{id}` has an unsafe upstream path `{path}`"));
            }
        }
    }
    if entry.items.is_empty() {
        return Err(format!("entry `{id}` lists no items"));
    }
    let mut keys = BTreeSet::new();
    for item in &entry.items {
        let label = format!("{} `{}` of entry `{id}`", item.kind.as_str(), item.id);
        if !keys.insert((item.kind, item.id.as_str())) {
            return Err(format!("{label} is listed twice"));
        }
        if !crate::is_canonical_sha256(&item.hash) {
            return Err(format!("{label} has a malformed hash"));
        }
        if item.kind == PackItemKind::Service {
            return Err(format!(
                "{label} cannot be offered: {}",
                crate::manifest::NON_BUNDLED_SERVICES_NOTE
            ));
        }
        if item.kind == PackItemKind::Mcp {
            let Some(definition) = &item.definition else {
                return Err(format!("{label} carries no definition"));
            };
            if item.path.is_some() || item.from.is_some() {
                return Err(format!("{label} is an MCP server but names a path"));
            }
            if crate::hash_mcp_definition(definition)? != item.hash {
                return Err(format!(
                    "{label} carries a definition that does not hash to its listed hash"
                ));
            }
        } else {
            let Some(path) = &item.path else {
                return Err(format!("{label} names no path"));
            };
            let dir = path.split('/').next().unwrap_or_default();
            if !is_safe_relative(path)
                || !(crate::CONTENT_DIRS.contains(&dir) || crate::PACK_LAYER_DIRS.contains(&dir))
            {
                return Err(format!("{label} has an unsafe path `{path}`"));
            }
            if item
                .from
                .as_deref()
                .is_some_and(|from| from.is_empty() || !is_safe_relative(from))
            {
                return Err(format!("{label} has an unsafe source path"));
            }
        }
        let derived = ItemRuns::classify(item.kind, item.definition.as_ref());
        if item.runs != derived {
            return Err(format!(
                "{label} is listed as `{}`, but it is `{}`",
                item.runs.as_str(),
                derived.as_str()
            ));
        }
    }
    Ok(())
}

/// Human description of where an entry's bytes came from, for refusals.
pub fn origin_description(source: &IndexSource, entry: &IndexEntry) -> String {
    match &entry.origin {
        EntryOrigin::Source { .. } => format!("{} at {}", source.url, source.commit),
        EntryOrigin::Upstream { url, commit, .. } => format!("upstream {url} at {commit}"),
    }
}

/// Check that the pack root staged at `root` is exactly `entry`: the same item
/// set, every item hash, and the pack's content hash.
pub fn verify_staged(
    entry: &IndexEntry,
    root: &Path,
    origin: &str,
) -> Result<crate::PackManifest, String> {
    let from_paths: BTreeMap<String, String> = entry
        .items
        .iter()
        .filter_map(|item| Some((item.path.clone()?, item.from.clone()?)))
        .collect();
    let staged = super::derive::entry_from_staged(
        &entry.id,
        entry.origin.clone(),
        root,
        &from_paths,
        Vec::new(),
    )
    .map_err(|e| format!("pack `{}` from {origin} could not be read: {e}", entry.id))?;

    let expected: BTreeMap<_, _> = entry
        .items
        .iter()
        .map(|item| ((item.kind, item.id.as_str()), item))
        .collect();
    let actual: BTreeMap<_, _> = staged
        .items
        .iter()
        .map(|item| ((item.kind, item.id.as_str()), item))
        .collect();
    for ((kind, id), item) in &expected {
        let Some(found) = actual.get(&(*kind, *id)) else {
            return Err(format!(
                "the signed index lists {} `{id}`, but {origin} does not contain it; nothing \
                 was installed",
                kind.as_str()
            ));
        };
        if found.path != item.path {
            return Err(format!(
                "{} `{id}` from {origin} installs to {:?}, but the signed index says {:?}; \
                 nothing was installed",
                kind.as_str(),
                found.path,
                item.path
            ));
        }
        if found.hash != item.hash {
            return Err(format!(
                "{} `{id}` from {origin} hashes to {}, but the signed index says {}; nothing \
                 was installed",
                kind.as_str(),
                found.hash,
                item.hash
            ));
        }
    }
    if let Some((kind, id)) = actual.keys().find(|key| !expected.contains_key(*key)) {
        return Err(format!(
            "{origin} contains {} `{id}`, which the signed index does not list; nothing was \
             installed",
            kind.as_str()
        ));
    }
    if staged.content_hash != entry.content_hash {
        return Err(format!(
            "pack `{}` from {origin} hashes to {}, but the signed index says {}; nothing was \
             installed",
            entry.id, staged.content_hash, entry.content_hash
        ));
    }
    crate::manifest::load(root)
}

#[cfg(test)]
mod tests {
    use super::super::index::IndexItem;
    use super::super::test_support::{signed_source, TestSigner};
    use super::*;

    const URL: &str = "https://github.com/bleugreenlab/cairn-library";
    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    fn hash(text: &str) -> String {
        crate::sha256_bytes(text.as_bytes())
    }

    fn entry(id: &str) -> IndexEntry {
        IndexEntry {
            id: id.to_string(),
            name: id.to_string(),
            version: "1.0.0".into(),
            description: String::new(),
            author: None,
            homepage: None,
            license: None,
            keywords: vec![],
            format: crate::PackFormat::Cairn,
            origin: EntryOrigin::Source {
                path: format!("plugins/{id}"),
            },
            content_hash: hash(id),
            items: vec![IndexItem {
                kind: PackItemKind::Skill,
                id: id.to_string(),
                path: Some(format!("skills/{id}")),
                from: None,
                hash: hash("skill"),
                runs: ItemRuns::Content,
                definition: None,
            }],
            notes: vec![],
        }
    }

    fn index(serial: u64, entries: Vec<IndexEntry>) -> LibraryIndex {
        LibraryIndex {
            format: INDEX_FORMAT.into(),
            source: IndexSource {
                url: URL.into(),
                git_ref: "main".into(),
                commit: COMMIT.into(),
            },
            serial,
            generated_at: "2026-11-02T00:00:00Z".into(),
            entries,
            notes: vec![],
        }
    }

    fn signed(signer: &TestSigner, index: &LibraryIndex) -> (Vec<u8>, String) {
        let bytes = index.to_canonical_json().unwrap().into_bytes();
        let signature = signer.sign(&bytes);
        (bytes, signature)
    }

    #[test]
    fn a_signed_index_verifies_and_names_its_signer() {
        let signer = TestSigner::new("cairn-library-test", 7);
        let source = signed_source(URL, &signer);
        let (bytes, signature) = signed(&signer, &index(3, vec![entry("linear")]));
        let verified = verify_index(&source, &bytes, &signature, 0).unwrap();
        assert_eq!(verified.signer, "cairn-library-test");
        assert!(verified.entry("linear").is_some());
        assert!(verified.refused.is_empty());
    }

    #[test]
    fn one_flipped_byte_breaks_the_signature() {
        let signer = TestSigner::new("k", 7);
        let source = signed_source(URL, &signer);
        let (mut bytes, signature) = signed(&signer, &index(3, vec![entry("linear")]));
        let at = bytes.len() / 2;
        bytes[at] ^= 1;
        let error = verify_index(&source, &bytes, &signature, 0).unwrap_err();
        assert_eq!(
            error,
            "index signature does not verify against the Test library's trusted keys"
        );
    }

    #[test]
    fn another_key_and_no_key_are_both_refused() {
        let signer = TestSigner::new("k", 7);
        let stranger = TestSigner::new("stranger", 9);
        let source = signed_source(URL, &signer);
        let (bytes, signature) = signed(&stranger, &index(3, vec![entry("linear")]));
        assert!(verify_index(&source, &bytes, &signature, 0)
            .unwrap_err()
            .contains("does not verify"));

        let keyless = crate::library::source::cairn_library();
        let error = verify_index(&keyless, &bytes, &signature, 0).unwrap_err();
        assert!(error.contains("no verification key"), "{error}");
    }

    #[test]
    fn a_replayed_index_for_another_repository_is_refused() {
        let signer = TestSigner::new("k", 7);
        let source = signed_source("https://github.com/other/library", &signer);
        let (bytes, signature) = signed(&signer, &index(3, vec![entry("linear")]));
        let error = verify_index(&source, &bytes, &signature, 0).unwrap_err();
        assert!(
            error.contains("replayed from another repository"),
            "{error}"
        );
    }

    #[test]
    fn an_older_serial_is_a_rollback() {
        let signer = TestSigner::new("k", 7);
        let source = signed_source(URL, &signer);
        let (bytes, signature) = signed(&signer, &index(3, vec![entry("linear")]));
        assert!(verify_index(&source, &bytes, &signature, 3).is_ok());
        let error = verify_index(&source, &bytes, &signature, 4).unwrap_err();
        assert!(error.contains("refusing a rollback"), "{error}");
    }

    #[test]
    fn an_abbreviated_commit_and_a_duplicate_entry_refuse_the_whole_index() {
        let signer = TestSigner::new("k", 7);
        let source = signed_source(URL, &signer);
        let mut short = index(3, vec![entry("linear")]);
        short.source.commit = "0123456".into();
        let (bytes, signature) = signed(&signer, &short);
        assert!(verify_index(&source, &bytes, &signature, 0)
            .unwrap_err()
            .contains("not a full 40-hex commit"));

        let (bytes, signature) = signed(&signer, &index(3, vec![entry("a"), entry("a")]));
        assert!(verify_index(&source, &bytes, &signature, 0)
            .unwrap_err()
            .contains("more than once"));
    }

    #[test]
    fn an_index_listing_a_machine_service_is_refused_by_name() {
        let signer = TestSigner::new("k", 7);
        let source = signed_source(URL, &signer);
        let mut carrying = entry("kit");
        carrying.items = vec![IndexItem {
            kind: PackItemKind::Service,
            id: "probe".into(),
            path: Some("services/probe".into()),
            from: None,
            hash: hash("service"),
            runs: ItemRuns::Here,
            definition: None,
        }];
        let (bytes, signature) = signed(&signer, &index(3, vec![carrying, entry("linear")]));
        let verified = verify_index(&source, &bytes, &signature, 0).unwrap();
        assert_eq!(verified.index.entries.len(), 1);
        assert_eq!(verified.refused[0].id, "kit");
        assert!(
            verified.refused[0]
                .reason
                .contains(crate::manifest::NON_BUNDLED_SERVICES_NOTE),
            "{}",
            verified.refused[0].reason
        );
    }

    #[test]
    fn a_bad_entry_is_refused_by_name_and_the_rest_are_offered() {
        let signer = TestSigner::new("k", 7);
        let source = signed_source(URL, &signer);
        let mut lying = entry("server");
        let definition: crate::server_config::McpServerConfig =
            serde_yaml::from_str("command: npx\n").unwrap();
        lying.items = vec![IndexItem {
            kind: PackItemKind::Mcp,
            id: "server".into(),
            path: None,
            from: None,
            hash: crate::hash_mcp_definition(&definition).unwrap(),
            // A stdio server described as inert.
            runs: ItemRuns::Content,
            definition: Some(definition),
        }];
        let (bytes, signature) = signed(
            &signer,
            &index(3, vec![entry("core"), lying, entry("linear")]),
        );
        let verified = verify_index(&source, &bytes, &signature, 0).unwrap();
        assert_eq!(
            verified
                .index
                .entries
                .iter()
                .map(|e| e.id.as_str())
                .collect::<Vec<_>>(),
            vec!["linear"]
        );
        let reasons: BTreeMap<_, _> = verified
            .refused
            .iter()
            .map(|r| (r.id.as_str(), r.reason.as_str()))
            .collect();
        assert_eq!(
            reasons["core"],
            "`core` is bundled with the app; a library entry may not replace it"
        );
        assert!(
            reasons["server"].contains("listed as `content`, but it is `here`"),
            "{reasons:?}"
        );
    }
}
