//! Which library sources this build reads, and how each one is trusted.

/// How a source's entries come to be believed.
///
/// Slice B adds a variant for third-party sources (an index derived at a pinned
/// commit, behind the operator-only setting of tenet 1370). This slice has no
/// such variant and no constructor that could build one, so a third-party
/// source is absent by construction rather than switched off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceTrust {
    /// The source publishes `index.json` with a minisign signature on
    /// `index_ref`, and the signature must verify against one of `keys`.
    SignedIndex {
        index_ref: String,
        keys: Vec<TrustedKey>,
    },
}

/// One key a signed index may be signed with. Rotation ships a release carrying
/// the old and new keys before CI switches which one signs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedKey {
    /// Recorded on the lock as the install's `signer`.
    pub id: String,
    /// The base64 line of a minisign public key file.
    pub minisign_public_key: String,
}

/// A git repository the Library reads entries from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibrarySource {
    /// Stable, filesystem-safe: the cache lives at `library/<id>/`.
    pub id: String,
    /// What the Library calls this source ("Cairn library").
    pub label: String,
    pub url: String,
    /// The branch whose tree entries are installed from.
    pub git_ref: String,
    pub trust: SourceTrust,
}

impl LibrarySource {
    /// The ref the signed index is published on.
    pub fn index_ref(&self) -> &str {
        match &self.trust {
            SourceTrust::SignedIndex { index_ref, .. } => index_ref,
        }
    }

    /// Whether `url` names this source's repository.
    pub fn is_url(&self, url: &str) -> bool {
        same_repository(&self.url, url)
    }
}

pub const CAIRN_LIBRARY_ID: &str = "cairn-library";
pub const CAIRN_LIBRARY_URL: &str = "https://github.com/bleugreenlab/cairn-library";

/// Production keys for the hosted library, `(id, minisign public key)`.
///
/// Created by the operator's key ceremony (see `docs/resource-packs.md`,
/// Signing). With no key compiled in, every index is refused as unverifiable,
/// which is the safe failure: the Library shows the bundled core and names why
/// the hosted entries are missing.
const CAIRN_LIBRARY_KEYS: &[(&str, &str)] = &[];

/// Cairn's hosted library: entries live on `main`; CI publishes the signed
/// index on the orphan `cairn-index` branch.
pub fn cairn_library() -> LibrarySource {
    LibrarySource {
        id: CAIRN_LIBRARY_ID.to_string(),
        label: "Cairn library".to_string(),
        url: CAIRN_LIBRARY_URL.to_string(),
        git_ref: "main".to_string(),
        trust: SourceTrust::SignedIndex {
            index_ref: "cairn-index".to_string(),
            keys: CAIRN_LIBRARY_KEYS
                .iter()
                .map(|(id, key)| TrustedKey {
                    id: (*id).to_string(),
                    minisign_public_key: (*key).to_string(),
                })
                .collect(),
        },
    }
}

/// Every source this build reads. The only place a source is named.
pub fn configured_sources() -> Vec<LibrarySource> {
    vec![cairn_library()]
}

/// The configured source a lock's `source.url` came from, if it is still
/// configured. A URL lock with no match keeps working but cannot update.
pub fn source_for_url<'a>(sources: &'a [LibrarySource], url: &str) -> Option<&'a LibrarySource> {
    sources.iter().find(|source| source.is_url(url))
}

/// Packs that used to ship in the app bundle and now live in the hosted
/// library. A bundled lock for one of these flips to the library's provenance
/// once the library's verified index carries it.
///
/// `matlab` is not among them: it ships a machine service, and a library
/// source never carries one, so it stays bundled.
pub const MOVED_TO_LIBRARY: &[&str] = &[
    "linear",
    "sentry",
    "notion",
    "supabase",
    "cloudflare",
    "vercel",
    "stripe",
    "deepwiki",
    "wolfram",
];

/// Two repository URLs name the same repository, ignoring a trailing `/` or
/// `.git` and the host's case.
pub fn same_repository(left: &str, right: &str) -> bool {
    normalize_repository(left) == normalize_repository(right)
}

fn normalize_repository(url: &str) -> String {
    let trimmed = url.trim().trim_end_matches('/');
    let trimmed = trimmed.strip_suffix(".git").unwrap_or(trimmed);
    match url::Url::parse(trimmed) {
        Ok(parsed) => format!(
            "{}://{}{}",
            parsed.scheme(),
            parsed.host_str().unwrap_or("").to_ascii_lowercase(),
            parsed.path().trim_end_matches('/')
        ),
        Err(_) => trimmed.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exactly_one_source_is_configured_and_it_is_signed() {
        let sources = configured_sources();
        assert_eq!(sources.len(), 1);
        let library = &sources[0];
        assert_eq!(library.url, "https://github.com/bleugreenlab/cairn-library");
        assert_eq!(library.git_ref, "main");
        assert_eq!(library.index_ref(), "cairn-index");
        assert!(matches!(library.trust, SourceTrust::SignedIndex { .. }));
    }

    #[test]
    fn locks_resolve_to_their_source_by_repository() {
        let sources = configured_sources();
        assert!(source_for_url(
            &sources,
            "https://github.com/bleugreenlab/cairn-library.git"
        )
        .is_some());
        assert!(
            source_for_url(&sources, "https://GitHub.com/bleugreenlab/cairn-library/").is_some()
        );
        assert!(source_for_url(&sources, "https://github.com/someone/else").is_none());
    }

    #[test]
    fn the_moved_packs_are_the_nine_bundled_connector_packs() {
        assert_eq!(MOVED_TO_LIBRARY.len(), 9);
        assert!(!MOVED_TO_LIBRARY.contains(&"core"));
        assert!(
            !MOVED_TO_LIBRARY.contains(&"matlab"),
            "a pack shipping a machine service cannot move to a library source"
        );
    }
}
