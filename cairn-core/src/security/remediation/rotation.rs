//! What the operator has to do at the provider, and why nothing here can do it
//! for them.
//!
//! Revocation reaches this process's willingness to hand a credential out again.
//! It does not reach the provider that accepts it. A GitHub installation token
//! copied out of a lease before the disclosure was noticed keeps working at
//! GitHub until GitHub expires it, no matter what this runner decides
//! afterwards. So for every credential a third party validates, **rotation at
//! that third party is the only step that ends the disclosure**, and it is a
//! step Cairn cannot take: rotating a Linear key means signing into Linear.
//!
//! Rather than leave that as a footnote, an incident carries a [`RotationHook`]
//! naming the provider, where the credential is configured, and what rotating it
//! means for that provider specifically. The incident stays
//! `rotation_required` until an operator says they have done it, so a response
//! that revoked leases and quarantined records does not read as finished while
//! the credential is still live at its provider.
//!
//! # Why rotation is last
//!
//! Rotating first would make the disclosure unfindable. The inventory matches by
//! registered value, and a rotated credential is never registered again — so the
//! records carrying the old one become invisible to the scan while remaining
//! exactly as readable to anyone who opens the database. See
//! [`super::inventory`].

use cairn_common::security::{SecretCategory, SecretId};

/// What an operator must rotate, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationHook {
    /// The provider that issued the credential and is the only place it can be
    /// invalidated.
    pub provider: &'static str,
    /// Where the replacement is configured in Cairn, so the instruction is
    /// actionable rather than a category name.
    pub configured_at: &'static str,
    /// What rotating means for this provider.
    pub guidance: &'static str,
    /// Whether revoking the lease already ends the credential's life. True only
    /// where Cairn's deadline and the provider's are the same moment.
    pub revocation_suffices: bool,
}

/// The rotation hook for a disclosed credential.
///
/// Dispatches on the `SecretId` prefix, which `CredentialSource::secret_id`
/// mints in one place. A test below drives every `CredentialSource` variant
/// through this function, so a new credential producer that ships without
/// rotation guidance fails the build rather than producing an incident that
/// tells the operator nothing.
pub fn rotation_hook(secret_id: &SecretId, category: Option<SecretCategory>) -> RotationHook {
    let id = secret_id.as_str();

    if id.starts_with("github-app:") {
        return RotationHook {
            provider: "github",
            configured_at: "Settings \u{2192} Integrations \u{2192} GitHub App",
            guidance:
                "Generate a new private key for the GitHub App and delete the disclosed one in \
                 the App's settings. Every installation token already minted from the old key \
                 stays valid until it expires, so revoke the installation's tokens too if the \
                 App's settings offer it.",
            revocation_suffices: false,
        };
    }
    if id.starts_with("github-installation:") {
        return RotationHook {
            provider: "github",
            configured_at: "Settings \u{2192} Integrations \u{2192} GitHub App",
            guidance: "An installation token is short-lived and Cairn leases it for exactly the \
                 deadline GitHub gave it, so revoking the lease and waiting out that deadline \
                 ends it. Rotate the App's private key as well if the disclosure could have \
                 reached it.",
            revocation_suffices: true,
        };
    }
    if id.starts_with("mcp-oauth:") {
        return RotationHook {
            provider: "mcp-server",
            configured_at: "Settings \u{2192} MCP servers",
            guidance: "Disconnect and re-authorize the server so the OAuth grant is re-issued. \
                 Revoking Cairn's copy does not revoke the token at the authorization server.",
            revocation_suffices: false,
        };
    }
    if id.starts_with("mcp-server:") {
        return RotationHook {
            provider: "mcp-server",
            configured_at: "Settings \u{2192} MCP servers",
            guidance:
                "Issue a new key at the upstream service, then replace the value Cairn resolves \
                 \u{2014} in the keychain field for a stored secret, or in the environment for a \
                 `${VAR}` the server declares. Cairn expands the reference per call, so the new \
                 value takes effect on the next round without a restart.",
            revocation_suffices: false,
        };
    }
    if id.starts_with("web-provider:") {
        return RotationHook {
            provider: "web-provider",
            configured_at: "Settings \u{2192} Web search and fetch",
            guidance:
                "Issue a new API key at the search or fetch provider, revoke the disclosed one \
                 there, and store the replacement.",
            revocation_suffices: false,
        };
    }
    if id.starts_with("model-backend:") {
        return RotationHook {
            provider: "model-backend",
            configured_at: "Settings \u{2192} Accounts",
            guidance:
                "Revoke the disclosed API key at the model provider and sign in again, or store \
                 a replacement key. A backend key is injected into the agent process's own \
                 environment, so revoking the lease governs re-issuance only \u{2014} a process \
                 already holding it is unaffected until it restarts.",
            revocation_suffices: false,
        };
    }
    if id.starts_with("team-sync:") {
        return RotationHook {
            provider: "cairn-cloud",
            configured_at: "Settings \u{2192} Account",
            guidance:
                "A team sync token is minted against this device's account credential. Signing \
                 the device out and back in mints a new one; if the device credential itself \
                 was disclosed, rotate that instead.",
            revocation_suffices: true,
        };
    }
    if id == "account-device" {
        return RotationHook {
            provider: "cairn-cloud",
            configured_at: "Settings \u{2192} Account",
            guidance:
                "Sign this device out of the Cairn account and sign back in. Every team sync \
                 token minted from the disclosed credential is revoked with it.",
            revocation_suffices: false,
        };
    }
    if id.starts_with("batch-capability:") {
        return RotationHook {
            provider: "cairn-runner",
            configured_at: "none \u{2014} minted per batch",
            guidance:
                "A per-batch executor relay capability is authorized only for the batch that \
                 minted it and is released when that batch ends, so it is already dead. No \
                 operator action is required.",
            revocation_suffices: true,
        };
    }
    if id.starts_with("callback:") || category == Some(SecretCategory::CallbackCredential) {
        return RotationHook {
            provider: "cairn-runner",
            configured_at: "none \u{2014} minted per run",
            guidance:
                "The MCP callback credential is minted by this runner and re-minted on restart. \
                 Restarting the runner ends the disclosed one.",
            revocation_suffices: false,
        };
    }

    // A producer this function has not been taught about. Deliberately loud
    // rather than silent: telling an operator "we do not know how to rotate
    // this" is actionable, and claiming no rotation is needed is not.
    RotationHook {
        provider: "unknown",
        configured_at: "unknown",
        guidance:
            "Cairn has no rotation guidance for this credential producer. Identify where the \
             value is issued and rotate it there; treat the credential as live until you have.",
        revocation_suffices: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::broker::CredentialSource;

    /// Every credential producer in the broker. Listed exhaustively so a new
    /// variant is a compile error here (the destructuring match below) rather
    /// than a silent gap in rotation guidance.
    fn every_source() -> Vec<CredentialSource> {
        let all = vec![
            CredentialSource::McpVar {
                credential_key: "linear".to_string(),
                var: "LINEAR_API_KEY".to_string(),
            },
            CredentialSource::McpOAuth {
                credential_key: "linear".to_string(),
            },
            CredentialSource::WebProvider {
                provider: "bmd".to_string(),
                var: "BMD_KEY".to_string(),
            },
            CredentialSource::GitHubApp { app_id: 1 },
            CredentialSource::GitHubInstallation { installation_id: 2 },
            CredentialSource::ModelBackend {
                provider: "anthropic".to_string(),
                account_id: "acct".to_string(),
            },
            CredentialSource::AccountDevice,
            CredentialSource::TeamSync {
                team_id: "team".to_string(),
            },
        ];
        // The exhaustiveness check: adding a variant to `CredentialSource`
        // without adding it above stops compiling here.
        for source in &all {
            match source {
                CredentialSource::McpVar { .. }
                | CredentialSource::McpOAuth { .. }
                | CredentialSource::WebProvider { .. }
                | CredentialSource::GitHubApp { .. }
                | CredentialSource::GitHubInstallation { .. }
                | CredentialSource::ModelBackend { .. }
                | CredentialSource::AccountDevice
                | CredentialSource::TeamSync { .. } => {}
            }
        }
        all
    }

    #[test]
    fn every_credential_producer_has_rotation_guidance() {
        for source in every_source() {
            let hook = rotation_hook(&source.secret_id(), None);
            assert_ne!(
                hook.provider, "unknown",
                "{:?} resolves to no rotation guidance, so an incident about it would tell \
                 the operator nothing",
                source
            );
            assert!(hook.guidance.len() > 40);
            assert!(!hook.configured_at.is_empty());
        }
    }

    #[test]
    fn an_unknown_producer_says_so_rather_than_claiming_safety() {
        let hook = rotation_hook(&SecretId::new("something-new:1"), None);
        assert_eq!(hook.provider, "unknown");
        assert!(!hook.revocation_suffices);
    }

    #[test]
    fn only_provider_bounded_credentials_claim_revocation_suffices() {
        // A credential Cairn injects into a process, or one a third party keeps
        // accepting, must not report that revoking the lease ended it.
        for id in [
            "model-backend:anthropic:acct",
            "mcp-server:linear:LINEAR_API_KEY",
            "web-provider:bmd:BMD_KEY",
            "github-app:1",
            "account-device",
        ] {
            assert!(
                !rotation_hook(&SecretId::new(id), None).revocation_suffices,
                "{id} claims revocation is enough, but its provider still accepts the value"
            );
        }
        // The installation token is the genuine exception: Cairn leases it for
        // exactly the deadline GitHub minted it with.
        assert!(rotation_hook(&SecretId::new("github-installation:2"), None).revocation_suffices);
    }
}
