//! Moving a workspace configured against the compiled provider catalogs onto
//! pack providers, once, at boot.
//!
//! The rule names no provider. A compiled provider's key lived at the account
//! `{provider}/{VAR}`, and each bundled provider pack's id and credential `var`
//! equal that provider's id and secret var, so the move is derived from the
//! bundled declarations alone:
//!
//! 1. **Keys.** For every bundled pack credential, a value at `{pack}/{var}`
//!    moves to `pack:{pack}/{credential}/{var}` (never overwriting a value
//!    already there) and the old entry is deleted. Every bundled pack, not only
//!    the active one, so a stored-but-unselected key is not lost. Skipped when
//!    a workspace MCP server named `{pack}` references `${VAR}`, because that
//!    account is the server's.
//! 2. **Install.** When a slot scalar names a bundled pack that provides the
//!    slot, is not installed, and was not uninstalled by the operator, the pack
//!    is installed. A pack that carries a machine service is never installed
//!    here: installing one is the operator's act alone, and the slot scalars
//!    are writable with the session token.
//! 3. **Settings** need nothing: the scalars and `webSearch.<pack>` options
//!    already have the right shape.
//!
//! Each step is a no-op once done, so running it every boot is the idempotence.
//! A keychain failure leaves the old entry in place for the next boot.
//!
//! The move is one-way: after it, an older build finds `{pack}/{VAR}` empty.

use std::path::Path;

use cairn_packs::provider::{self, Slot};
use cairn_packs::PackItemKind;
use cairn_platform::services::{FileSystem, GitClient};

use crate::security::broker::{adopt_legacy_key, LegacyKeyAdoption};

/// What one migration pass changed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// `{pack}/{VAR}` accounts whose value now lives in the pack namespace.
    pub keys_moved: Vec<String>,
    /// Packs installed because a slot scalar named them.
    pub installed: Vec<String>,
    /// What was deliberately left alone, and why.
    pub skipped: Vec<String>,
}

impl MigrationReport {
    pub fn changed(&self) -> bool {
        !self.keys_moved.is_empty() || !self.installed.is_empty()
    }
}

/// Run the migration against `config_dir` with the app's bundled packs at
/// `resource_dir`.
pub fn migrate_workspace(
    git: &dyn GitClient,
    fs: &dyn FileSystem,
    config_dir: &Path,
    resource_dir: &Path,
) -> MigrationReport {
    let mut report = MigrationReport::default();
    let servers = crate::config::mcp_servers::load_workspace_mcp_servers(config_dir);
    for manifest in cairn_packs::discover_available_packs(resource_dir) {
        let credentials = match provider::load_pack_credentials(&manifest.root, &manifest.id) {
            Ok(credentials) => credentials,
            Err(error) => {
                log::warn!(
                    "web providers: bundled pack `{}` skipped: {error}",
                    manifest.id
                );
                continue;
            }
        };
        for credential in credentials {
            let account = format!("{}/{}", manifest.id, credential.var);
            if servers
                .get(&manifest.id)
                .is_some_and(|server| server.referenced_vars().contains(&credential.var))
            {
                report.skipped.push(format!(
                    "{account}: the MCP server `{}` uses it",
                    manifest.id
                ));
                continue;
            }
            match adopt_legacy_key(
                &manifest.id,
                &manifest.id,
                &credential.name,
                &credential.var,
            ) {
                Ok(LegacyKeyAdoption::Moved) => report.keys_moved.push(account),
                Ok(LegacyKeyAdoption::Absent | LegacyKeyAdoption::Superseded) => {}
                Err(error) => {
                    log::warn!("web providers: could not move the key at {account}: {error}")
                }
            }
        }
    }

    for slot in [Slot::WebSearch, Slot::WebFetch] {
        let Some(name) = super::active_name(config_dir, slot) else {
            continue;
        };
        if super::builtin_default(slot) == Some(name.as_str())
            || cairn_packs::lock::read_lock(config_dir, &name).is_some()
        {
            continue;
        }
        let Some(manifest) = cairn_packs::available_pack(resource_dir, &name) else {
            continue;
        };
        if !matches!(
            provider::load_pack_provider(&manifest.root, &name, slot),
            Ok(Some(_))
        ) {
            continue;
        }
        if cairn_packs::lock::is_uninstalled(config_dir, &name) {
            report
                .skipped
                .push(format!("{name}: uninstalled by the operator"));
            continue;
        }
        if manifest
            .items()
            .iter()
            .any(|item| item.kind == PackItemKind::Service)
        {
            log::warn!(
                "web providers: `{}` names pack `{name}`, which carries a machine service; \
                 installing it is the operator's act, so it is left for the Library",
                super::scalar_key(slot)
            );
            report
                .skipped
                .push(format!("{name}: carries a machine service"));
            continue;
        }
        match cairn_packs::sync_one_pack(git, fs, resource_dir, config_dir, &name) {
            Ok(_) => report.installed.push(name),
            Err(error) => log::warn!("web providers: could not install `{name}`: {error}"),
        }
    }
    report
}
