//! The seam between the public facet and subscription mechanics.
//!
//! Subscription mechanics — CLI sign-in, where a managed Claude account's home
//! lives, renewing a Codex subscription's rotating token, probing a
//! subscription's usage — are not part of the public facet. The runtime reaches
//! them only through [`Subscriptions`], which the private
//! `cairn-llm-subscriptions` crate implements. [`LlmHost`] is the reverse
//! direction: the host services that implementation needs, provided by the
//! runtime without the private crate depending on it.
//!
//! A build without the private crate installs [`Unsubscribed`], which answers
//! every subscription question honestly: there is no support for it here.

use std::path::PathBuf;

use cairn_platform::services::process::ProcessSpawner;

use crate::accounts::{CredentialStoreHandle, RoutedProvider};
pub use crate::claude::tokens::ClaudeSessionCredential;
use crate::codex::tokens::CodexAuthState;
use crate::codex::tokens::{CodexAccountRef, CodexRefreshFailure, CodexRefreshRequest};
use crate::interface::session::{Secret, SecretEnv};
use crate::types::usage::{ProviderUsageResetResult, ProviderUsageSnapshot};
use serde_json::Value;

/// What a build without subscription mechanics says when asked for them.
pub const NO_SUBSCRIPTION_SUPPORT: &str = "this build of Cairn has no subscription support";

/// Host services subscription mechanics run against. Implemented by the
/// runtime (core's `Orchestrator`), and by any host outside Cairn that installs
/// subscription support.
///
/// Every method is required, so a host states each answer rather than
/// inheriting a silent one.
pub trait LlmHost: Send + Sync {
    /// The credential store this process routes from. Its `config_dir` is
    /// also where sign-in keeps its scratch homes.
    fn store(&self) -> &CredentialStoreHandle;
    /// How child processes (the provider CLIs) are started.
    fn process_spawner(&self) -> &dyn ProcessSpawner;
    /// The Claude CLI this host runs sessions with.
    fn claude_path(&self) -> Result<String, String>;
    /// The Codex CLI this host signs in and probes usage with, checked to be
    /// new enough (see `cairn_llm::session::codex::version`).
    fn codex_path(&self) -> Result<String, String>;
    /// A credential was published to the store; surfaces that show accounts
    /// should re-read.
    fn credentials_published(&self);
    /// Register a credential value so it is scrubbed wherever it is echoed.
    ///
    /// Sign-in and refresh call this with every token they handle. A host
    /// that drops these leaves those tokens readable in anything it echoes
    /// (logs, tool output, a UI), which is why there is no default.
    fn register_secret(&self, provider: &str, key: &str, value: &str);
}

/// Subscription mechanics, as the runtime asks for them.
pub trait Subscriptions: Send + Sync {
    /// Where a managed Claude account's CLI home lives, if this build places
    /// one. Resolving never creates it.
    fn claude_home(&self, host: &dyn LlmHost, account_id: &str) -> Option<PathBuf>;
    /// Create (or repair) a managed Claude account's home, ready to sign in or
    /// to run a session in.
    fn prepare_claude_home(&self, host: &dyn LlmHost, account_id: &str) -> Result<PathBuf, String>;
    /// Whether the store holds a credential for that account Cairn can renew.
    fn claude_home_signed_in(&self, host: &dyn LlmHost, account_id: &str) -> bool;
    /// Sign the account out (revoking its refresh token) and remove its home,
    /// before its row is removed.
    fn release_claude_home(&self, host: &dyn LlmHost, account_id: &str) -> Result<(), String>;
    /// What a Claude process launched on this managed account receives in its
    /// environment to authenticate as it — an access token with enough life
    /// left that the process never needs to refresh, never a refresh token —
    /// and which generation of the credential that is.
    fn claude_session_credential(
        &self,
        host: &dyn LlmHost,
        account_id: &str,
    ) -> Result<ClaudeSessionCredential, String>;
    /// Renew every managed Claude account close to expiry. Answers with the
    /// accounts whose generation advanced; the runtime pushes each one's new
    /// token into its live processes.
    fn renew_claude_credentials(&self, host: &dyn LlmHost) -> Vec<String>;
    /// A process on `account_id`, fed `generation`, was refused with a 401.
    /// Answers with the credential to push to it: the current one if the
    /// account has moved on, a renewed one if `generation` is still current.
    fn claude_credential_rejected(
        &self,
        host: &dyn LlmHost,
        account_id: &str,
        generation: u64,
    ) -> Result<ClaudeSessionCredential, String>;
    /// Take over every managed Claude home signed in before Cairn held its
    /// tokens, scrubbing the CLI's copy. Answers with the accounts taken over.
    fn migrate_claude_homes(&self, host: &dyn LlmHost) -> Vec<String>;

    /// Probe a subscription's usage. `None` names the default account. The
    /// second value fingerprints the credential generation the probe read, so
    /// a result that crossed a credential rewrite can be discarded.
    fn probe_usage(
        &self,
        host: &dyn LlmHost,
        provider: RoutedProvider,
        account_id: Option<&str>,
    ) -> (ProviderUsageSnapshot, Option<String>);
    /// The credential generation a probe of this account would read now.
    fn credential_generation(
        &self,
        host: &dyn LlmHost,
        provider: RoutedProvider,
        account_id: &str,
    ) -> Option<String>;
    /// Spend one of the subscription's usage-reset credits.
    fn consume_usage_reset(
        &self,
        host: &dyn LlmHost,
        provider: RoutedProvider,
        account_id: Option<&str>,
    ) -> Result<ProviderUsageResetResult, String>;

    /// The `account/login/start` params that sign a Codex app-server in as
    /// the subscription `auth` holds.
    fn codex_session_login(
        &self,
        host: &dyn LlmHost,
        auth: &CodexAuthState,
    ) -> Result<Secret<Value>, String>;

    /// Renew a managed Codex account's credential under the machine-wide
    /// single-spender contract, answering with the credential as it is now
    /// durable and the account it belongs to. The caller's
    /// [`crate::codex::tokens::CodexAuthState`] adopts the JSON.
    fn refresh_codex(
        &self,
        host: &dyn LlmHost,
        request: CodexRefreshRequest,
    ) -> Result<(String, CodexAccountRef), CodexRefreshFailure>;
}

/// The subscription answer of a build without the private crate.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unsubscribed;

fn unsupported_usage(provider: RoutedProvider) -> ProviderUsageSnapshot {
    ProviderUsageSnapshot::unsupported(
        provider.backend(),
        "subscription_usage",
        NO_SUBSCRIPTION_SUPPORT,
    )
}

impl Subscriptions for Unsubscribed {
    fn claude_home(&self, _host: &dyn LlmHost, _account_id: &str) -> Option<PathBuf> {
        None
    }

    fn prepare_claude_home(
        &self,
        _host: &dyn LlmHost,
        _account_id: &str,
    ) -> Result<PathBuf, String> {
        Err(format!(
            "Managed Claude accounts are unavailable: {NO_SUBSCRIPTION_SUPPORT}."
        ))
    }

    fn claude_home_signed_in(&self, _host: &dyn LlmHost, _account_id: &str) -> bool {
        false
    }

    fn release_claude_home(&self, _host: &dyn LlmHost, _account_id: &str) -> Result<(), String> {
        // Nothing was ever placed, so there is nothing to sign out.
        Ok(())
    }

    fn claude_session_credential(
        &self,
        host: &dyn LlmHost,
        account_id: &str,
    ) -> Result<ClaudeSessionCredential, String> {
        self.prepare_claude_home(host, account_id)
            .map(|_| ClaudeSessionCredential {
                env: SecretEnv::new(),
                generation: 0,
            })
    }

    fn renew_claude_credentials(&self, _host: &dyn LlmHost) -> Vec<String> {
        Vec::new()
    }

    fn claude_credential_rejected(
        &self,
        _host: &dyn LlmHost,
        _account_id: &str,
        _generation: u64,
    ) -> Result<ClaudeSessionCredential, String> {
        Err(format!(
            "Managed Claude accounts are unavailable: {NO_SUBSCRIPTION_SUPPORT}."
        ))
    }

    fn migrate_claude_homes(&self, _host: &dyn LlmHost) -> Vec<String> {
        Vec::new()
    }

    fn probe_usage(
        &self,
        _host: &dyn LlmHost,
        provider: RoutedProvider,
        _account_id: Option<&str>,
    ) -> (ProviderUsageSnapshot, Option<String>) {
        (unsupported_usage(provider), None)
    }

    fn credential_generation(
        &self,
        _host: &dyn LlmHost,
        _provider: RoutedProvider,
        _account_id: &str,
    ) -> Option<String> {
        None
    }

    fn consume_usage_reset(
        &self,
        _host: &dyn LlmHost,
        _provider: RoutedProvider,
        _account_id: Option<&str>,
    ) -> Result<ProviderUsageResetResult, String> {
        Err(format!(
            "Usage resets are unavailable: {NO_SUBSCRIPTION_SUPPORT}."
        ))
    }

    fn codex_session_login(
        &self,
        _host: &dyn LlmHost,
        _auth: &CodexAuthState,
    ) -> Result<Secret<Value>, String> {
        Err(format!(
            "Codex subscription accounts are unavailable: {NO_SUBSCRIPTION_SUPPORT}."
        ))
    }

    fn refresh_codex(
        &self,
        _host: &dyn LlmHost,
        _request: CodexRefreshRequest,
    ) -> Result<(String, CodexAccountRef), CodexRefreshFailure> {
        Err(CodexRefreshFailure::unrecoverable(
            None,
            NO_SUBSCRIPTION_SUPPORT,
        ))
    }
}

/// Test doubles for hosts that exercise account homes without the private
/// crate.
#[cfg(any(test, feature = "test-utils"))]
pub mod testing {
    use super::*;

    /// Places managed Claude homes anywhere under `root`, and answers
    /// "signed in" from a marker file [`TempHomes::sign_in`] writes. Every
    /// other subscription question gets [`Unsubscribed`]'s answer.
    #[derive(Debug, Clone)]
    pub struct TempHomes {
        pub root: PathBuf,
    }

    const SIGNED_IN_MARKER: &str = ".signed-in";

    impl TempHomes {
        pub fn new(root: impl Into<PathBuf>) -> Self {
            Self { root: root.into() }
        }

        pub fn home(&self, account_id: &str) -> PathBuf {
            self.root.join(account_id)
        }

        /// Mark an account's home signed in.
        pub fn sign_in(&self, account_id: &str) {
            let home = self.home(account_id);
            std::fs::create_dir_all(&home).expect("create test home");
            std::fs::write(home.join(SIGNED_IN_MARKER), "").expect("mark test home");
        }
    }

    impl Subscriptions for TempHomes {
        fn claude_home(&self, _host: &dyn LlmHost, account_id: &str) -> Option<PathBuf> {
            Some(self.home(account_id))
        }

        fn prepare_claude_home(
            &self,
            _host: &dyn LlmHost,
            account_id: &str,
        ) -> Result<PathBuf, String> {
            let home = self.home(account_id);
            std::fs::create_dir_all(&home).map_err(|err| err.to_string())?;
            Ok(home)
        }

        fn claude_home_signed_in(&self, _host: &dyn LlmHost, account_id: &str) -> bool {
            self.home(account_id).join(SIGNED_IN_MARKER).exists()
        }

        fn release_claude_home(&self, _host: &dyn LlmHost, account_id: &str) -> Result<(), String> {
            let home = self.home(account_id);
            if home.exists() {
                std::fs::remove_dir_all(&home).map_err(|err| err.to_string())?;
            }
            Ok(())
        }

        /// The test home, presented the way a managed home is.
        fn claude_session_credential(
            &self,
            host: &dyn LlmHost,
            account_id: &str,
        ) -> Result<ClaudeSessionCredential, String> {
            let home = self.prepare_claude_home(host, account_id)?;
            Ok(ClaudeSessionCredential {
                env: SecretEnv::new().with("CLAUDE_CONFIG_DIR", home.to_string_lossy()),
                generation: 0,
            })
        }

        fn renew_claude_credentials(&self, host: &dyn LlmHost) -> Vec<String> {
            Unsubscribed.renew_claude_credentials(host)
        }

        fn claude_credential_rejected(
            &self,
            host: &dyn LlmHost,
            account_id: &str,
            generation: u64,
        ) -> Result<ClaudeSessionCredential, String> {
            Unsubscribed.claude_credential_rejected(host, account_id, generation)
        }

        fn migrate_claude_homes(&self, host: &dyn LlmHost) -> Vec<String> {
            Unsubscribed.migrate_claude_homes(host)
        }

        fn probe_usage(
            &self,
            host: &dyn LlmHost,
            provider: RoutedProvider,
            account_id: Option<&str>,
        ) -> (ProviderUsageSnapshot, Option<String>) {
            Unsubscribed.probe_usage(host, provider, account_id)
        }

        fn credential_generation(
            &self,
            host: &dyn LlmHost,
            provider: RoutedProvider,
            account_id: &str,
        ) -> Option<String> {
            Unsubscribed.credential_generation(host, provider, account_id)
        }

        fn consume_usage_reset(
            &self,
            host: &dyn LlmHost,
            provider: RoutedProvider,
            account_id: Option<&str>,
        ) -> Result<ProviderUsageResetResult, String> {
            Unsubscribed.consume_usage_reset(host, provider, account_id)
        }

        fn codex_session_login(
            &self,
            host: &dyn LlmHost,
            auth: &CodexAuthState,
        ) -> Result<Secret<Value>, String> {
            Unsubscribed.codex_session_login(host, auth)
        }

        fn refresh_codex(
            &self,
            host: &dyn LlmHost,
            request: CodexRefreshRequest,
        ) -> Result<(String, CodexAccountRef), CodexRefreshFailure> {
            Unsubscribed.refresh_codex(host, request)
        }
    }
}
