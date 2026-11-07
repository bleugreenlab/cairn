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
//! 3. **OAuth records.** A compiled provider kept its OAuth record at
//!    `{slot}/{pack}` in the OAuth store. For every bundled pack `oauth`
//!    credential and each `provider:<slot>` accessor it lists, a record there
//!    moves to the credential's account `pack:{pack}/{credential}` (never
//!    overwriting one already there), and the old record and its `/pending`
//!    entry are deleted. Derived from the declarations, so no provider is
//!    named here.
//! 4. **Settings** need nothing: the scalars and `webSearch.<pack>` /
//!    `webFetch.<pack>` options already have the right shape. A stored select
//!    value a declaration no longer offers falls back to its default at use.
//!
//! An installed bundled pack that gains a provider in a new app version needs
//! no step here: the boot sync of bundled packs installs newly shipped items of
//! an installed pack the way it updates changed ones.
//!
//! Each step is a no-op once done, so running it every boot is the idempotence.
//! A keychain failure leaves the old entry in place for the next boot.
//!
//! The move is one-way: after it, an older build finds `{pack}/{VAR}` and
//! `{slot}/{pack}` empty.

use super::oauth::{adopt_legacy_record, LegacyRecordAdoption};

use std::path::Path;

use cairn_packs::provider::{self, CredentialKind, Slot};
use cairn_packs::PackItemKind;
use cairn_platform::services::{FileSystem, GitClient};

use crate::security::broker::{adopt_legacy_key, LegacyKeyAdoption};

/// What one migration pass changed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// `{pack}/{VAR}` accounts whose value now lives in the pack namespace.
    pub keys_moved: Vec<String>,
    /// `{slot}/{pack}` OAuth records that now live at the pack credential.
    pub oauth_moved: Vec<String>,
    /// Packs installed because a slot scalar named them.
    pub installed: Vec<String>,
    /// What was deliberately left alone, and why.
    pub skipped: Vec<String>,
}

impl MigrationReport {
    pub fn changed(&self) -> bool {
        !self.keys_moved.is_empty() || !self.oauth_moved.is_empty() || !self.installed.is_empty()
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
            let var = match &credential.kind {
                CredentialKind::Secret { var } => var,
                CredentialKind::OAuth { .. } => {
                    for slot in &credential.accessors {
                        let legacy = format!("{}/{}", slot.as_str(), manifest.id);
                        match adopt_legacy_record(&legacy, &credential, &manifest.id) {
                            Ok(LegacyRecordAdoption::Moved) => report.oauth_moved.push(legacy),
                            Ok(LegacyRecordAdoption::Absent | LegacyRecordAdoption::Superseded) => {}
                            Err(error) => log::warn!(
                                "web providers: could not move the OAuth record at {legacy}: {error}"
                            ),
                        }
                    }
                    continue;
                }
            };
            let account = format!("{}/{var}", manifest.id);
            if servers
                .get(&manifest.id)
                .is_some_and(|server| server.referenced_vars().contains(var))
            {
                report.skipped.push(format!(
                    "{account}: the MCP server `{}` uses it",
                    manifest.id
                ));
                continue;
            }
            match adopt_legacy_key(&manifest.id, &manifest.id, &credential.name, var) {
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
