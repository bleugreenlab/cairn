//! The source-sensitive check implementation identity of this build.
//!
//! `build.rs` hashes the workspace-relative path and bytes of every Rust source,
//! `Cargo.toml` and `Cargo.lock` under `src-tauri`, so equal source trees give
//! equal identities on every machine and checkout path. Verdict reuse keys and
//! the runner build id carry it (`cairn_checkenv::implementation_identity`).
//!
//! The hash reruns on any edit, so it lives in this leaf crate that only binary
//! packages depend on. Each binary hands the value to `cairn-checkenv` with
//! [`install`] first in `main`; the libraries that read it never compile against
//! it, and an edit outside them leaves them fresh.

/// This build's implementation identity, `source-v1:` and a 64-bit FNV-1a hash.
pub const IMPLEMENTATION_ID: &str = env!("CAIRN_CHECK_IMPLEMENTATION_ID");

/// Install [`IMPLEMENTATION_ID`] as this process's implementation identity.
/// Every binary that links `cairn-checkenv` calls it first in `main`
/// (`tests/installers.rs` holds them to it).
pub fn install() {
    cairn_checkenv::set_implementation_identity(IMPLEMENTATION_ID);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_hands_checkenv_the_build_scripts_identity() {
        install();
        assert_eq!(cairn_checkenv::implementation_identity(), IMPLEMENTATION_ID);
        assert!(
            IMPLEMENTATION_ID.starts_with("source-v1:"),
            "{IMPLEMENTATION_ID}"
        );
        // Installing the same value again is harmless.
        install();
    }
}
