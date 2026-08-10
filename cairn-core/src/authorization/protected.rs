//! The workspace configuration document, and what may reach it.
//!
//! `~/.cairn/settings.yaml` is the one file on the host whose *contents* decide
//! what every future agent in this workspace can do: which providers and models
//! work is routed to, which credentials are in play, which commands the host
//! will run, which paths the sandbox refuses to read, which MCP servers are
//! wired in. Authorization for a change to it therefore has to be evaluated
//! semantically -- this section, this server, this configuration -- which is
//! what the `cairn://settings` and `cairn://mcp` mutation surfaces do.
//!
//! A byte-level write cannot be adjudicated that way. A shell redirect and a
//! `file:` change item both land bytes without ever naming a capability, and
//! reproducing each write mode's transformation here to work out what they
//! *would* land would put a second implementation of the apply path between an
//! agent and this file -- where any divergence authorizes one document while
//! writing another.
//!
//! So the rule is structural: **the workspace configuration document is not
//! writable by an agent except through the brokered surfaces.** It is enforced
//! at two boundaries, the structured write handler and the process boundary,
//! and neither consults the filesystem Fence. Containment decides whether a
//! process may cross a filesystem boundary; this decides whether the
//! workspace's own capability set may be rewritten without naming what is
//! changing. `Fence::Allow` relaxes the first and must never answer the second.
//!
//! A second file is protected here for a closely related reason.
//! `~/.cairn/operator_auth_secret` is the credential that distinguishes a real
//! desktop operator answering an authority prompt from anything else on the
//! machine that can open a loopback socket. Unlike the configuration document,
//! it is protected against **reads** as well as writes, because reading it is
//! the whole attack: an agent holding those bytes can approve its own
//! escalation. And unlike an ordinary secret, a fence prompt is not an adequate
//! guard for it — allowing a containment crossing is something an agent may do
//! through its own `permissions` resource, so a merely deny-read credential
//! would be one self-approved crossing away from disclosure. Both of its
//! boundaries therefore refuse rather than prompt.

use std::path::{Component, Path, PathBuf};

/// What an agent is told when it tries to write the configuration document
/// directly. Names the surface that works, because a refusal an agent cannot
/// act on just becomes a retry loop.
pub const BROKERED_ONLY_REFUSAL: &str = concat!(
    "Denied: the workspace configuration document (~/.cairn/settings.yaml) cannot be written ",
    "directly, because its contents decide what every future agent in this workspace can do and ",
    "a raw file write does not say which capability is changing. Use the brokered surfaces, ",
    "which validate the change and name the authority it needs: `write cairn://settings` for ",
    "settings sections, and `write cairn://mcp` for MCP servers. This is not a containment ",
    "decision, so it is not affected by the filesystem fence."
);

/// What an agent is told when it tries to read or write the desktop operator
/// credential. Says plainly that there is no version of this that succeeds, so
/// it does not read as a transient failure worth retrying.
pub const OPERATOR_CREDENTIAL_REFUSAL: &str = concat!(
    "Denied: the desktop operator credential (~/.cairn/operator_auth_secret) is not readable or ",
    "writable by an agent. It is the credential that lets the person at this machine approve an ",
    "authority prompt, so an agent holding it could approve its own escalation. This is not a ",
    "containment decision and it is not approvable: no fence answer, `allowAll`, or permission ",
    "response makes this path reachable. An authority request is answered by the operator in the ",
    "desktop app."
);

/// The workspace configuration document for this install.
pub fn workspace_settings_path(config_dir: &Path) -> PathBuf {
    config_dir.join("settings.yaml")
}

/// The desktop operator credential for this install.
pub fn operator_credential_path(config_dir: &Path) -> PathBuf {
    config_dir.join(cairn_common::auth::OPERATOR_SECRET_FILE)
}

/// Resolve `..` and `.` without touching the filesystem.
///
/// Done as a separate step from canonicalization because the two catch
/// different things and neither subsumes the other: this one still answers for
/// a path whose parent does not exist, where `canonicalize` gives up entirely.
fn lexically_normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Canonicalize as much of `path` as exists on disk, keeping the rest verbatim.
///
/// Plain `canonicalize` fails outright on a path that does not exist yet, which
/// is the common case here: a create targeting a settings file that is not
/// there, or one reached through a directory symlink. Walking up to the deepest
/// existing ancestor and re-appending the remainder resolves every symlink,
/// alias, and `/var` -> `/private/var` indirection that IS present, which is
/// what makes the comparison one of identity rather than spelling.
fn best_effort_canonical(path: &Path) -> PathBuf {
    if let Ok(resolved) = path.canonicalize() {
        return resolved;
    }
    let mut trailing = Vec::new();
    let mut cursor = lexically_normalize(path);
    while let (Some(name), Some(parent)) = (
        cursor.file_name().map(ToOwned::to_owned),
        cursor.parent().map(Path::to_path_buf),
    ) {
        trailing.push(name);
        if let Ok(resolved) = parent.canonicalize() {
            let mut out = resolved;
            for part in trailing.iter().rev() {
                out.push(part);
            }
            return out;
        }
        cursor = parent;
    }
    lexically_normalize(path)
}

/// Compare two resolved paths, case-insensitively where the platform's default
/// filesystem is. On macOS and Windows `Settings.YAML` opens the same file as
/// `settings.yaml`, so a byte comparison would be a bypass rather than a
/// nicety.
fn same_path(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    if cfg!(target_os = "macos") || cfg!(target_os = "windows") {
        let (Some(left), Some(right)) = (left.to_str(), right.to_str()) else {
            return false;
        };
        return left.eq_ignore_ascii_case(right);
    }
    false
}

/// Whether `candidate` resolves to the workspace configuration document.
///
/// Compares resolved identity, not spelling, in both directions: a symlink or
/// alias pointing at the document is the document, and so is a path that only
/// differs by `..`, a `/var` indirection, or filename case on a
/// case-insensitive volume. The lexical comparison is a second net for the case
/// where nothing on the path exists yet and canonicalization has nothing to
/// resolve.
///
/// Deliberately narrow: it names one file, not the config directory. `~/.cairn`
/// holds build slots, scratch space, and caches that agents legitimately write
/// all day, and a directory-wide rule would break ordinary work while adding no
/// authority protection.
///
/// Known limit: this resolves *paths*, so it does not see a **hard link**.
/// `canonicalize` follows symlinks but hard links share an inode without
/// sharing a path, and the file write path truncates in place — so a write
/// through one would reach the document. Creating the link needs a shell
/// command, which the sandbox governs under `Fence::Ask`, so it is not
/// obviously reachable; it is recorded here so the next reader does not assume
/// resolution-by-path covers it.
pub fn is_workspace_settings_path(candidate: &Path, config_dir: &Path) -> bool {
    resolves_to(candidate, &workspace_settings_path(config_dir))
}

/// Whether `candidate` resolves to the desktop operator credential.
///
/// Same identity comparison as [`is_workspace_settings_path`], and it inherits
/// the same hard-link limit.
pub fn is_operator_credential_path(candidate: &Path, config_dir: &Path) -> bool {
    resolves_to(candidate, &operator_credential_path(config_dir))
}

/// Whether two paths name the same file, comparing resolved identity rather
/// than spelling in both directions.
fn resolves_to(candidate: &Path, protected: &Path) -> bool {
    same_path(
        &best_effort_canonical(candidate),
        &best_effort_canonical(protected),
    ) || same_path(
        &lexically_normalize(candidate),
        &lexically_normalize(protected),
    )
}

/// The refusal for a host path a process was blocked from touching, or `None`
/// when the denial is an ordinary containment crossing the fence should
/// adjudicate.
///
/// Called at the process boundary, where allowing a crossing re-executes the
/// command with the sandbox switched off. For these two paths that answer is
/// never right: the containment prompt only ever says "a file outside the
/// project is being touched", so approving it would hand over the workspace's
/// capability set, or the credential that approves changes to it, on the
/// strength of a question that never mentioned either.
pub fn denied_path_refusal(config_dir: &Path, path: &Path) -> Option<&'static str> {
    if is_workspace_settings_path(path, config_dir) {
        return Some(BROKERED_ONLY_REFUSAL);
    }
    if is_operator_credential_path(path, config_dir) {
        return Some(OPERATOR_CREDENTIAL_REFUSAL);
    }
    None
}

/// The refusal for a **read** of a protected host path, or `None`.
///
/// Narrower than [`denied_path_refusal`] on purpose. Reading the configuration
/// document is ordinary, useful work — an agent inspecting how the workspace is
/// set up is not expanding anything. Reading the operator credential has no
/// legitimate form at all, because its only use is to authenticate as the
/// operator.
pub fn read_refusal(config_dir: &Path, path: &Path) -> Option<&'static str> {
    is_operator_credential_path(path, config_dir).then_some(OPERATOR_CREDENTIAL_REFUSAL)
}

// ============================================================================
// Reaching the document through a structured `write`
// ============================================================================

/// Every host path a change item would touch.
///
/// Usually just its target, but a `unified_patch` carries its own paths inside
/// the envelope and is typically addressed at the bare worktree root -- so
/// looking only at the item's target would miss an envelope section naming the
/// configuration document outright.
///
/// An unparseable envelope contributes no paths: the apply path rejects it with
/// the parse error, and inventing paths from a malformed one would refuse valid
/// work for the wrong reason.
fn change_item_paths(item: &crate::mcp::types::ChangeItem, residence: &Path) -> Vec<PathBuf> {
    use crate::mcp::file_targets::normalize_change_target;

    let mut targets: Vec<String> = vec![item.target.clone()];
    if item.mode == crate::mcp::types::ChangeMode::UnifiedPatch {
        if let Some(patch) = item
            .payload
            .as_ref()
            .and_then(|payload| payload.get("patch"))
            .and_then(|patch| patch.as_str())
        {
            if let Ok(sections) = crate::mcp::diff::parse_patch_envelope(patch) {
                for section in sections {
                    let path = section.path();
                    targets.push(if path.starts_with("file:") {
                        path.to_string()
                    } else {
                        format!("file:{path}")
                    });
                }
            }
        }
    }

    targets
        .iter()
        .filter_map(|target| {
            // `allow_escape: true` deliberately. The question here is "where
            // would this land", not "may it land there" -- normalizing with
            // escapes refused would hide precisely the absolute host paths this
            // check exists to catch.
            let normalized = normalize_change_target(target, true).ok()?;
            let rest = normalized.strip_prefix("file:")?;
            if rest.is_empty() {
                return None;
            }
            // Joined rather than resolved through the filesystem. The resolver
            // canonicalizes the residence first and fails outright when it
            // cannot, which would make an unresolvable working directory
            // silently answer "nothing here is protected" -- a fail-OPEN on the
            // one check that must never have one.
            Some(if rest.starts_with('/') {
                PathBuf::from(rest)
            } else {
                residence.join(rest)
            })
        })
        .collect()
}

/// The refusal for a structured change that would reach the workspace
/// configuration document, or `None` when it would not.
///
/// Called for every item in a batch before any of them applies, so a mixed
/// batch is refused whole rather than half-applied. Non-file targets resolve to
/// nothing and cost a parse.
pub fn structured_change_refusal(
    config_dir: &Path,
    residence: &Path,
    item: &crate::mcp::types::ChangeItem,
) -> Option<String> {
    change_item_paths(item, residence)
        .iter()
        .find_map(|path| denied_path_refusal(config_dir, path))
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_document_itself_is_protected_however_it_is_spelled() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".cairn");
        std::fs::create_dir_all(&config).unwrap();
        let settings = config.join("settings.yaml");
        std::fs::write(&settings, "logLevel: verbose\n").unwrap();

        assert!(is_workspace_settings_path(&settings, &config));
        // Traversal that lands on the same file.
        assert!(is_workspace_settings_path(
            &config.join("agents").join("..").join("settings.yaml"),
            &config
        ));
        // A relative-looking detour through the parent.
        assert!(is_workspace_settings_path(
            &dir.path().join(".cairn/./settings.yaml"),
            &config
        ));
    }

    #[test]
    fn a_symlink_to_the_document_is_the_document() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".cairn");
        std::fs::create_dir_all(&config).unwrap();
        let settings = config.join("settings.yaml");
        std::fs::write(&settings, "logLevel: verbose\n").unwrap();

        let link = dir.path().join("innocuous.yaml");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&settings, &link).unwrap();
        #[cfg(not(unix))]
        std::fs::copy(&settings, &link).unwrap();

        #[cfg(unix)]
        assert!(
            is_workspace_settings_path(&link, &config),
            "a symlink pointing at the document must resolve to it, or the rule is about \
             spelling rather than about the file"
        );
    }

    #[test]
    fn a_symlinked_config_directory_still_resolves_to_the_document() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real-cairn");
        std::fs::create_dir_all(&real).unwrap();
        std::fs::write(real.join("settings.yaml"), "logLevel: verbose\n").unwrap();

        let linked = dir.path().join("linked-cairn");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&real, &linked).unwrap();
            assert!(is_workspace_settings_path(
                &linked.join("settings.yaml"),
                &real
            ));
        }
        let _ = linked;
    }

    #[test]
    fn an_absent_document_is_still_protected() {
        // A create targeting a settings file that does not exist yet is the
        // most direct way to install one, so it cannot depend on the file
        // already being there to be recognized.
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".cairn");
        std::fs::create_dir_all(&config).unwrap();
        assert!(is_workspace_settings_path(
            &config.join("settings.yaml"),
            &config
        ));
    }

    #[test]
    fn ordinary_neighbours_are_not_protected() {
        // The config directory is full of things agents legitimately write.
        // Catching them here would break ordinary work and protect nothing.
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".cairn");
        std::fs::create_dir_all(config.join("build-slots")).unwrap();
        for ordinary in [
            config.join("settings.yaml.bak"),
            config.join("agents/build.md"),
            config.join("build-slots/CAIRN/slot-1/src/lib.rs"),
            dir.path().join("project/settings.yaml"),
            dir.path().join("project/.cairn/config.yaml"),
        ] {
            assert!(
                !is_workspace_settings_path(&ordinary, &config),
                "{} must stay directly writable",
                ordinary.display()
            );
        }
    }

    #[test]
    fn the_operator_credential_is_refused_for_reads_and_writes_however_it_is_spelled() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".cairn");
        std::fs::create_dir_all(&config).unwrap();
        let credential = config.join(cairn_common::auth::OPERATOR_SECRET_FILE);
        std::fs::write(&credential, [0u8; 32]).unwrap();

        for spelling in [
            credential.clone(),
            config
                .join("agents")
                .join("..")
                .join(cairn_common::auth::OPERATOR_SECRET_FILE),
        ] {
            assert_eq!(
                read_refusal(&config, &spelling),
                Some(OPERATOR_CREDENTIAL_REFUSAL),
                "{} must be unreadable",
                spelling.display()
            );
            assert_eq!(
                denied_path_refusal(&config, &spelling),
                Some(OPERATOR_CREDENTIAL_REFUSAL),
                "{} must be untouchable at the process boundary",
                spelling.display()
            );
        }
    }

    #[test]
    fn a_symlink_to_the_operator_credential_is_the_credential() {
        // The credential's whole value is that an agent cannot obtain the
        // bytes, so a rule about spelling rather than about the file would be
        // no rule at all.
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".cairn");
        std::fs::create_dir_all(&config).unwrap();
        let credential = config.join(cairn_common::auth::OPERATOR_SECRET_FILE);
        std::fs::write(&credential, [0u8; 32]).unwrap();

        let link = dir.path().join("notes.txt");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&credential, &link).unwrap();
            assert_eq!(
                read_refusal(&config, &link),
                Some(OPERATOR_CREDENTIAL_REFUSAL)
            );
        }
        let _ = link;
    }

    #[test]
    fn reading_the_configuration_document_stays_ordinary_work() {
        // The two protected paths are protected for different reasons and must
        // not be collapsed: inspecting how the workspace is configured expands
        // nothing, while reading the operator credential has no benign form.
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".cairn");
        std::fs::create_dir_all(&config).unwrap();

        assert_eq!(read_refusal(&config, &config.join("settings.yaml")), None);
        assert_eq!(
            denied_path_refusal(&config, &config.join("settings.yaml")),
            Some(BROKERED_ONLY_REFUSAL)
        );
    }

    #[test]
    fn an_ordinary_neighbour_of_the_credential_is_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".cairn");
        std::fs::create_dir_all(&config).unwrap();

        for ordinary in [
            config.join("mcp_auth_secret"),
            config.join("operator_auth_secret.bak"),
            dir.path().join("project/operator_auth_secret"),
        ] {
            assert_eq!(
                read_refusal(&config, &ordinary),
                None,
                "{} must stay readable",
                ordinary.display()
            );
        }
    }

    #[test]
    fn a_project_settings_file_of_the_same_name_is_not_the_workspace_one() {
        // Two files can share a name; only one of them configures the
        // workspace. Identity is the resolved path, not the basename.
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("home/.cairn");
        let elsewhere = dir.path().join("elsewhere/.cairn");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::create_dir_all(&elsewhere).unwrap();
        assert!(!is_workspace_settings_path(
            &elsewhere.join("settings.yaml"),
            &config
        ));
    }
}
