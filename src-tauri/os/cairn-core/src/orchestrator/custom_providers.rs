//! The lifecycle of a workspace's named custom providers.
//!
//! A custom provider is one `ProviderAccount` under `ApiProvider::Custom` plus
//! one entry in `settings.enabled_providers`. Those two facts are meaningless
//! apart — an account nothing enables is a provider no surface shows, and an
//! enabled key naming no account is a provider that cannot run — so adding and
//! removing move both together here rather than leaving the pairing to a
//! caller to remember.

use crate::config::settings::ProviderNames;
use crate::credentials::{AccountInfo, ApiProvider, CustomHeader, CustomSampling, ProviderAuth};
use crate::models::{Settings, UpdateSettings};
use cairn_llm::types::catalog::{self, CustomProvider, ProviderDescriptor};

use super::Orchestrator;

impl Orchestrator {
    /// The custom providers this workspace has defined, in account priority
    /// order.
    pub fn custom_providers(&self, project_id: Option<&str>) -> Vec<CustomProvider> {
        self.get_credential_store()
            .map(|store| cairn_llm::catalog::custom_providers_in(&store, project_id))
            .unwrap_or_default()
    }

    /// This workspace's provider inventory: everything Cairn ships, then the
    /// custom providers defined here.
    pub fn provider_catalog(&self) -> Vec<ProviderDescriptor> {
        cairn_llm::catalog::workspace_catalog_for(self.get_credential_store().as_ref())
    }

    /// Provider display names for settings refusals.
    pub(crate) fn provider_names(&self) -> ProviderNames {
        ProviderNames::new(self.custom_providers(None).into_iter().map(|provider| {
            (
                catalog::custom_key(&provider.account_id),
                provider.display_name,
            )
        }))
    }

    /// Define a custom provider and install it in this workspace.
    ///
    /// The account is created first because its id is what the enabled key
    /// names. If enabling then fails — a workspace with no tier able to serve
    /// it, say — the account is removed again, so a refused add leaves nothing
    /// half-created behind.
    pub fn add_custom_provider(
        &self,
        label: String,
        auth: ProviderAuth,
        project_id: Option<String>,
    ) -> Result<AccountInfo, String> {
        let label = label.trim().to_string();
        if label.is_empty() {
            return Err("give this provider a name".to_string());
        }
        if !matches!(auth, ProviderAuth::CustomEndpoint { .. }) {
            return Err("a custom provider needs an endpoint".to_string());
        }
        let info = self.add_account(ApiProvider::Custom, label, auth, project_id)?;
        let key = catalog::custom_key(&info.id);
        let mut enabled = self.get_settings().enabled_providers;
        if !enabled.iter().any(|entry| entry == &key) {
            enabled.push(key);
        }
        if let Err(error) = self.update_settings(UpdateSettings {
            enabled_providers: Some(enabled),
            ..Default::default()
        }) {
            let _ = self.remove_account(&info.id);
            return Err(error);
        }
        Ok(info)
    }

    /// What one custom provider currently holds that an edit must not destroy:
    /// its bearer key, the values of its operator headers, its sampling
    /// controls, and its maximum output budget.
    ///
    /// Exists so that editing a provider can leave them alone. A stored
    /// credential never crosses the boundary to a UI, so a form that changes
    /// the address cannot echo the key back with it; without a way to carry the
    /// unchanged secrets forward, every rename or move would silently clear
    /// them. Sampling and the output budget are carried for a different reason:
    /// they are public, but a client written before either field existed omits
    /// it, and an omission must not read as a request to reset it.
    pub fn custom_provider_inherited_fields(
        &self,
        id: &str,
    ) -> Option<(Option<String>, Vec<CustomHeader>, CustomSampling, i64)> {
        let store = self.get_credential_store()?;
        store
            .accounts
            .iter()
            .find_map(|account| match &account.auth {
                ProviderAuth::CustomEndpoint {
                    api_key,
                    headers,
                    sampling,
                    max_output_tokens,
                    ..
                } if account.id == id && account.api_provider == ApiProvider::Custom => Some((
                    api_key.clone(),
                    headers.clone(),
                    sampling.clone(),
                    *max_output_tokens,
                )),
                _ => None,
            })
    }

    /// Change what a custom provider is called or where it lives.
    ///
    /// `auth` replaces the endpoint wholesale, because that is the shape it is
    /// validated in: an address, a protocol, a credential, and a model policy
    /// are checked against each other, so patching one of them in isolation
    /// could produce a combination the boundary would have refused. The account
    /// id never changes, so nothing that already selected this provider is
    /// disturbed by either edit.
    pub fn update_custom_provider(
        &self,
        id: &str,
        label: Option<String>,
        auth: Option<ProviderAuth>,
    ) -> Result<AccountInfo, String> {
        let mut store = self.get_credential_store().ok_or("No identity store")?;
        let account = store
            .accounts
            .iter_mut()
            .find(|account| account.id == id && account.api_provider == ApiProvider::Custom)
            .ok_or_else(|| format!("Custom provider not found: {id}"))?;
        if let Some(label) = label {
            let label = label.trim().to_string();
            if label.is_empty() {
                return Err("give this provider a name".to_string());
            }
            account.label = label;
        }
        if let Some(auth) = auth {
            if !matches!(auth, ProviderAuth::CustomEndpoint { .. }) {
                return Err("a custom provider needs an endpoint".to_string());
            }
            account.auth = auth;
        }
        let info = AccountInfo::from(&*account);
        self.save_credential_store(store)?;
        self.emit_config_changed();
        self.spawn_model_catalog_refresh();
        Ok(info)
    }

    /// Remove a custom provider from this workspace.
    ///
    /// `settings` is the same enablement change the removal dialog computes —
    /// the key dropped, plus any tier rebound onto another provider — and it is
    /// applied FIRST. A removal that would strand a tier is refused there, and
    /// refusing before the definition is destroyed is what makes the refusal
    /// recoverable.
    ///
    /// Unlike removing a first-class provider, this does delete the stored
    /// account. For every other provider the account is a credential that
    /// outlives the workspace's interest in it; here the account IS the
    /// provider, and keeping it would leave a definition nothing can reach.
    pub fn remove_custom_provider(
        &self,
        id: &str,
        settings: UpdateSettings,
    ) -> Result<Settings, String> {
        let exists = self.get_credential_store().is_some_and(|store| {
            store
                .accounts
                .iter()
                .any(|account| account.id == id && account.api_provider == ApiProvider::Custom)
        });
        if !exists {
            return Err(format!("Custom provider not found: {id}"));
        }
        let key = catalog::custom_key(id);
        if settings
            .enabled_providers
            .as_ref()
            .is_some_and(|enabled| enabled.iter().any(|entry| entry == &key))
        {
            return Err("the change still installs the provider being removed".to_string());
        }
        let updated = self.update_settings(settings)?;
        self.remove_account(id)?;
        Ok(updated)
    }
}
