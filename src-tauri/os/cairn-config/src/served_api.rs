//! `servedApi:` in `settings.yaml`: Cairn's OpenAI-compatible API for outside
//! clients on the local network. Clients name models by their real names
//! (tenet 1336); there is no alias table here.
//!
//! The listener is off unless `bind` is set (or the orch's
//! `--served-api-listen` flag names an address). Nothing here is tied to the
//! desktop shell: a headless orch reads the same section.

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// How long a non-streamed request may wait for a local model to load before
/// it is answered 503, when the section does not say.
pub const DEFAULT_LOAD_WAIT_SECS: u64 = 1200;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServedApiSettings {
    /// The socket address the listener binds, e.g. `0.0.0.0:80`. Absent means
    /// the served API is off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind: Option<String>,
    /// Terminate TLS in process with this certificate. Absent serves plain HTTP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<ServedApiTls>,
    /// The longest a request waits for a local model to load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_wait_secs: Option<u64>,
}

/// A PEM certificate chain and its private key, as files on the orch's host.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServedApiTls {
    pub cert_path: String,
    pub key_path: String,
}

impl ServedApiSettings {
    pub fn load_wait(&self) -> Duration {
        Duration::from_secs(self.load_wait_secs.unwrap_or(DEFAULT_LOAD_WAIT_SECS))
    }

    pub fn validate(&self) -> Result<(), String> {
        if let Some(bind) = &self.bind {
            validate_bind(bind).map_err(|error| format!("servedApi.bind: {error}"))?;
        }
        if let Some(tls) = &self.tls {
            if tls.cert_path.trim().is_empty() || tls.key_path.trim().is_empty() {
                return Err("servedApi.tls needs certPath and keyPath".into());
            }
        }
        if self.load_wait_secs == Some(0) {
            return Err("servedApi.loadWaitSecs must be at least 1".into());
        }
        Ok(())
    }
}

/// A bind address must be a literal socket address, so the operator sees
/// exactly which interface faces the network.
pub fn validate_bind(bind: &str) -> Result<SocketAddr, String> {
    bind.trim()
        .parse::<SocketAddr>()
        .map_err(|_| format!("{bind:?} is not an address:port such as 0.0.0.0:80"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(value: serde_json::Value) -> Result<ServedApiSettings, String> {
        let settings: ServedApiSettings =
            serde_json::from_value(value).map_err(|error| error.to_string())?;
        settings.validate()?;
        Ok(settings)
    }

    #[test]
    fn a_full_section_parses_and_defaults_the_load_wait() {
        let settings = parsed(serde_json::json!({
            "bind": "0.0.0.0:80",
            "tls": {"certPath": "/etc/cairn/api.pem", "keyPath": "/etc/cairn/api.key"}
        }))
        .unwrap();
        assert_eq!(settings.bind.as_deref(), Some("0.0.0.0:80"));
        assert_eq!(
            settings.load_wait(),
            Duration::from_secs(DEFAULT_LOAD_WAIT_SECS)
        );
    }

    #[test]
    fn unknown_keys_and_bad_values_are_refused() {
        assert!(parsed(serde_json::json!({"listen": "0.0.0.0:80"})).is_err());
        assert!(parsed(serde_json::json!({"bind": "llm.bglab.lan"}))
            .unwrap_err()
            .contains("address:port"));
        assert!(parsed(serde_json::json!({"tls": {"certPath": "", "keyPath": "k"}})).is_err());
        assert!(parsed(serde_json::json!({"loadWaitSecs": 0})).is_err());
        // Tenet 1336: models go by their real names, so there is no alias table.
        assert!(parsed(serde_json::json!({"modelAliases": {"big": "m"}})).is_err());
    }
}
