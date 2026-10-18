//! The TLS client configuration every `wss://` connection uses. (HTTP goes
//! through reqwest, with its own.)
//!
//! Built once with an explicit crypto provider rather than through
//! `ClientConfig::builder()`, which panics when a dependency graph compiles in
//! both of rustls' providers (Cairn's does). `ring` is the provider reqwest's
//! rustls feature already brings into the facet, so a standalone host gains no
//! second one through this crate.

use std::sync::{Arc, OnceLock};

use rustls::{ClientConfig, RootCertStore};

use crate::SpeechWireError;

pub(crate) fn client_config() -> Result<Arc<ClientConfig>, SpeechWireError> {
    static CONFIG: OnceLock<Result<Arc<ClientConfig>, String>> = OnceLock::new();
    CONFIG
        .get_or_init(build)
        .clone()
        .map_err(SpeechWireError::Transport)
}

fn build() -> Result<Arc<ClientConfig>, String> {
    let loaded = rustls_native_certs::load_native_certs();
    if !loaded.errors.is_empty() {
        log::warn!(
            "some native root certificates could not be loaded: {:?}",
            loaded.errors
        );
    }
    let mut roots = RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(loaded.certs);
    if added == 0 {
        return Err(
            "no native root certificates were found, so a TLS connection cannot be verified"
                .to_string(),
        );
    }
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| format!("TLS configuration failed: {error}"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(Arc::new(config))
}
