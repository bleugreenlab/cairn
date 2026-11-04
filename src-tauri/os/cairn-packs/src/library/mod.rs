//! Library sources: remote git repositories the Library reads entries from.
//!
//! A source is a git URL plus a ref. Its trust is a property of the source
//! ([`source::SourceTrust`]): the hosted Cairn library publishes an index that
//! CI signs, and the app verifies that signature against a compiled-in key
//! before believing a single entry. Everything else here is source-generic:
//! fetching ([`git`]), turning a marketplace layout into an index ([`derive`]),
//! staging an entry into a flat pack root ([`stage`]), and checking staged bytes
//! against the index ([`verify`]). Only [`source::configured_sources`] names a
//! source, and it names exactly one.
//!
//! This module touches the filesystem and the `git` binary and nothing else.
//! Orchestration (when to refresh, applying updates, emitting events) lives in
//! `cairn-core`.

pub mod cache;
pub mod derive;
pub mod git;
pub mod index;
pub mod marketplace;
pub mod source;
pub mod stage;
pub mod verify;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use index::{EntryOrigin, IndexEntry, IndexItem, IndexSource, LibraryIndex, INDEX_FORMAT};
pub use source::{configured_sources, LibrarySource, SourceTrust, TrustedKey, MOVED_TO_LIBRARY};
pub use verify::{EntryRefusal, VerifiedIndex};

/// Whether `value` is a full, lowercase 40-hex git commit id. Abbreviated
/// commits are refused everywhere: the commit is the trust anchor.
pub fn is_full_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Whether `path` is a plain relative path: no root, no `..`, no backslash, no
/// empty or `.` segments. The empty string (the repository root) is allowed.
pub fn is_safe_relative(path: &str) -> bool {
    if path.is_empty() {
        return true;
    }
    !path.starts_with('/')
        && !path.contains('\\')
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_full_lowercase_commits_count() {
        assert!(is_full_commit("0123456789abcdef0123456789abcdef01234567"));
        assert!(!is_full_commit("0123456"));
        assert!(!is_full_commit("0123456789ABCDEF0123456789abcdef01234567"));
        assert!(!is_full_commit("main"));
    }

    #[test]
    fn relative_paths_cannot_escape() {
        assert!(is_safe_relative(""));
        assert!(is_safe_relative("plugins/linear"));
        assert!(!is_safe_relative("/etc"));
        assert!(!is_safe_relative("plugins/../.."));
        assert!(!is_safe_relative("./plugins"));
        assert!(!is_safe_relative("plugins//x"));
        assert!(!is_safe_relative("a\\b"));
    }
}
