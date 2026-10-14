//! A workspace's provider inventory: everything Cairn ships, then the custom
//! providers its credential store defines.

use cairn_credentials::{ApiProvider, CredentialStore, ProviderAuth};
use cairn_llm_types::catalog::{workspace_catalog, CustomProvider, ProviderDescriptor};

/// The custom providers a store defines, in account priority order.
pub fn custom_providers_in(
    store: &CredentialStore,
    project_id: Option<&str>,
) -> Vec<CustomProvider> {
    store
        .accounts_for_provider(ApiProvider::Custom, project_id)
        .into_iter()
        .filter(|account| matches!(account.auth, ProviderAuth::CustomEndpoint { .. }))
        .map(|account| CustomProvider {
            account_id: account.id.clone(),
            display_name: account.label.clone(),
        })
        .collect()
}

/// Every provider surface reads this rather than the shipped constant, so a
/// custom provider gets the same tab, panel, picker presence, and tier
/// participation as a first-class one without any surface special-casing it.
pub fn workspace_catalog_for(store: Option<&CredentialStore>) -> Vec<ProviderDescriptor> {
    let customs = store
        .map(|store| custom_providers_in(store, None))
        .unwrap_or_default();
    workspace_catalog(&customs)
}
