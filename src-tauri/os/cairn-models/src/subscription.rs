//! The seam between the public facet and subscription mechanics.
//!
//! Subscription mechanics — CLI sign-in, where a managed Claude account's home
//! lives, renewing a Codex subscription's rotating token, probing a
//! subscription's usage — are not part of the public facet. The runtime reaches
//! them only through [`Subscriptions`], which the private
//! `cairn-models-subscriptions` crate implements. [`ModelHost`] is the reverse
//! direction: the host services that implementation needs, provided by the
//! runtime without the private crate depending on it.
//!
//! A build without the private crate installs [`Unsubscribed`], which answers
//! every subscription question honestly: there is no support for it here.

use std::path::PathBuf;
use std::sync::Arc;

use cairn_platform::services::process::ProcessSpawner;

use crate::accounts::{AccountInfo, CredentialStoreHandle, RoutedProvider};
pub use crate::claude::tokens::ClaudeSessionCredential;
use crate::codex::tokens::CodexAuthState;
use crate::codex::tokens::{CodexAccountRef, CodexRefreshFailure, CodexRefreshRequest};
use crate::interface::session::{Secret, SecretEnv};
pub use crate::sign_in::{
    store_codex_sign_in, PendingSignIn, SignInError, SignInFailureKind, SignInFuture,
    SignInOutcome, SignInRecordError, SignInRequest, SignInStart, SignInStatus, SignInTarget,
    SubmitCodeResult,
};
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
pub trait ModelHost: Send + Sync {
    /// The credential store this process routes from. Its `config_dir` is
    /// also where sign-in keeps its scratch homes.
    fn store(&self) -> &CredentialStoreHandle;
    /// How child processes (the provider CLIs) are started.
    fn process_spawner(&self) -> &dyn ProcessSpawner;
    /// The Claude CLI this host runs sessions with.
    fn claude_path(&self) -> Result<String, String>;
    /// The Codex CLI this host signs in and probes usage with, checked to be
    /// new enough (see `cairn_models::session::codex::version`).
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

    // The account rows a sign-in creates, completes, or discards.

    /// Whether `id` is a managed Claude account visible to `project_id`.
    fn is_managed_claude_account(&self, id: &str, project_id: Option<&str>) -> bool;
    /// Create the provisional account row a new Claude sign-in fills in.
    fn add_claude_profile_account(&self, project_id: Option<String>)
        -> Result<AccountInfo, String>;
    /// Remove an account, releasing its home. Best effort: a sign-in that is
    /// discarding its own provisional row has nothing to report if it fails.
    fn remove_account(&self, id: &str);
    /// Record a completed Claude sign-in's metadata, and start a usage capture.
    fn record_claude_sign_in(
        &self,
        id: &str,
        email: Option<String>,
        plan: Option<String>,
    ) -> Result<AccountInfo, String>;
    /// Store a completed Codex sign-in's `auth.json` under `target`, and start
    /// a usage capture. A completion `target` does not admit
    /// ([`SignInTarget::admits`]) is refused and nothing is stored.
    fn record_codex_sign_in(
        &self,
        auth_json: String,
        project_id: Option<String>,
        target: &SignInTarget,
    ) -> Result<AccountInfo, SignInRecordError>;
    /// A sign-in reached its terminal outcome. Called exactly once per sign-in
    /// reference, whichever path ended it: completion, failure, cancellation,
    /// timeout, or abandonment, including a start that failed outright.
    fn sign_in_settled(&self, sign_in_ref: &str, provider: RoutedProvider, outcome: SignInOutcome);
}

/// Subscription mechanics, as the runtime asks for them.
pub trait Subscriptions: Send + Sync {
    /// Where a managed Claude account's CLI home lives, if this build places
    /// one. Resolving never creates it.
    fn claude_home(&self, host: &dyn ModelHost, account_id: &str) -> Option<PathBuf>;
    /// Create (or repair) a managed Claude account's home, ready to sign in or
    /// to run a session in.
    fn prepare_claude_home(
        &self,
        host: &dyn ModelHost,
        account_id: &str,
    ) -> Result<PathBuf, String>;
    /// Whether the store holds a credential for that account Cairn can renew.
    fn claude_home_signed_in(&self, host: &dyn ModelHost, account_id: &str) -> bool;
    /// Sign the account out (revoking its refresh token) and remove its home,
    /// before its row is removed.
    fn release_claude_home(&self, host: &dyn ModelHost, account_id: &str) -> Result<(), String>;
    /// What a Claude process launched on this managed account receives in its
    /// environment to authenticate as it — an access token with enough life
    /// left that the process never needs to refresh, never a refresh token —
    /// and which generation of the credential that is.
    fn claude_session_credential(
        &self,
        host: &dyn ModelHost,
        account_id: &str,
    ) -> Result<ClaudeSessionCredential, String>;
    /// Renew every managed Claude account close to expiry. Answers with the
    /// accounts whose generation advanced; the runtime pushes each one's new
    /// token into its live processes.
    fn renew_claude_credentials(&self, host: &dyn ModelHost) -> Vec<String>;
    /// A process on `account_id`, fed `generation`, was refused with a 401.
    /// Answers with the credential to push to it: the current one if the
    /// account has moved on, a renewed one if `generation` is still current.
    fn claude_credential_rejected(
        &self,
        host: &dyn ModelHost,
        account_id: &str,
        generation: u64,
    ) -> Result<ClaudeSessionCredential, String>;
    /// Take over every managed Claude home signed in before Cairn held its
    /// tokens, scrubbing the CLI's copy. Answers with the accounts taken over.
    fn migrate_claude_homes(&self, host: &dyn ModelHost) -> Vec<String>;

    /// Probe a subscription's usage. `None` names the default account. The
    /// second value fingerprints the credential generation the probe read, so
    /// a result that crossed a credential rewrite can be discarded.
    fn probe_usage(
        &self,
        host: &dyn ModelHost,
        provider: RoutedProvider,
        account_id: Option<&str>,
    ) -> (ProviderUsageSnapshot, Option<String>);
    /// The credential generation a probe of this account would read now.
    fn credential_generation(
        &self,
        host: &dyn ModelHost,
        provider: RoutedProvider,
        account_id: &str,
    ) -> Option<String>;
    /// Spend one of the subscription's usage-reset credits.
    fn consume_usage_reset(
        &self,
        host: &dyn ModelHost,
        provider: RoutedProvider,
        account_id: Option<&str>,
    ) -> Result<ProviderUsageResetResult, String>;

    /// The `account/login/start` params that sign a Codex app-server in as
    /// the subscription `auth` holds.
    fn codex_session_login(
        &self,
        host: &dyn ModelHost,
        auth: &CodexAuthState,
    ) -> Result<Secret<Value>, String>;

    /// Renew a managed Codex account's credential under the machine-wide
    /// single-spender contract, answering with the credential as it is now
    /// durable and the account it belongs to. The caller's
    /// [`crate::codex::tokens::CodexAuthState`] adopts the JSON.
    fn refresh_codex(
        &self,
        host: &dyn ModelHost,
        request: CodexRefreshRequest,
    ) -> Result<(String, CodexAccountRef), CodexRefreshFailure>;

    /// Start signing a subscription account in, under `request.sign_in_ref`.
    /// Opens the user's browser on this machine. Every terminal outcome,
    /// including a start that fails here, reaches
    /// [`ModelHost::sign_in_settled`] exactly once.
    fn start_sign_in(
        &self,
        host: Arc<dyn ModelHost>,
        request: SignInRequest,
    ) -> SignInFuture<Result<SignInStart, SignInError>>;
    /// Has a pending sign-in finished? A finished one is handed out once, and
    /// asking retires it; a failed one answers with its error.
    fn sign_in_status(&self, sign_in_ref: &str) -> Result<SignInStatus, SignInError>;
    /// Hand a pasted code to a pending Claude sign-in.
    fn submit_sign_in_code(
        &self,
        host: Arc<dyn ModelHost>,
        sign_in_ref: String,
        code: String,
    ) -> SignInFuture<Result<SubmitCodeResult, SignInError>>;
    /// End a pending sign-in, discarding a new account's provisional row.
    /// Answers whether the cancel ended it: a sign-in already publishing its
    /// credential finishes, and is reported as it ends, not as cancelled.
    fn cancel_sign_in(&self, host: &dyn ModelHost, sign_in_ref: &str) -> bool;
    /// Sign-ins still in the user's hands, oldest first.
    fn pending_sign_ins(&self) -> Vec<PendingSignIn>;
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
    fn claude_home(&self, _host: &dyn ModelHost, _account_id: &str) -> Option<PathBuf> {
        None
    }

    fn prepare_claude_home(
        &self,
        _host: &dyn ModelHost,
        _account_id: &str,
    ) -> Result<PathBuf, String> {
        Err(format!(
            "Managed Claude accounts are unavailable: {NO_SUBSCRIPTION_SUPPORT}."
        ))
    }

    fn claude_home_signed_in(&self, _host: &dyn ModelHost, _account_id: &str) -> bool {
        false
    }

    fn release_claude_home(&self, _host: &dyn ModelHost, _account_id: &str) -> Result<(), String> {
        // Nothing was ever placed, so there is nothing to sign out.
        Ok(())
    }

    fn claude_session_credential(
        &self,
        host: &dyn ModelHost,
        account_id: &str,
    ) -> Result<ClaudeSessionCredential, String> {
        self.prepare_claude_home(host, account_id)
            .map(|_| ClaudeSessionCredential {
                env: SecretEnv::new(),
                generation: 0,
            })
    }

    fn renew_claude_credentials(&self, _host: &dyn ModelHost) -> Vec<String> {
        Vec::new()
    }

    fn claude_credential_rejected(
        &self,
        _host: &dyn ModelHost,
        _account_id: &str,
        _generation: u64,
    ) -> Result<ClaudeSessionCredential, String> {
        Err(format!(
            "Managed Claude accounts are unavailable: {NO_SUBSCRIPTION_SUPPORT}."
        ))
    }

    fn migrate_claude_homes(&self, _host: &dyn ModelHost) -> Vec<String> {
        Vec::new()
    }

    fn probe_usage(
        &self,
        _host: &dyn ModelHost,
        provider: RoutedProvider,
        _account_id: Option<&str>,
    ) -> (ProviderUsageSnapshot, Option<String>) {
        (unsupported_usage(provider), None)
    }

    fn credential_generation(
        &self,
        _host: &dyn ModelHost,
        _provider: RoutedProvider,
        _account_id: &str,
    ) -> Option<String> {
        None
    }

    fn consume_usage_reset(
        &self,
        _host: &dyn ModelHost,
        _provider: RoutedProvider,
        _account_id: Option<&str>,
    ) -> Result<ProviderUsageResetResult, String> {
        Err(format!(
            "Usage resets are unavailable: {NO_SUBSCRIPTION_SUPPORT}."
        ))
    }

    fn codex_session_login(
        &self,
        _host: &dyn ModelHost,
        _auth: &CodexAuthState,
    ) -> Result<Secret<Value>, String> {
        Err(format!(
            "Codex subscription accounts are unavailable: {NO_SUBSCRIPTION_SUPPORT}."
        ))
    }

    fn refresh_codex(
        &self,
        _host: &dyn ModelHost,
        _request: CodexRefreshRequest,
    ) -> Result<(String, CodexAccountRef), CodexRefreshFailure> {
        Err(CodexRefreshFailure::unrecoverable(
            None,
            NO_SUBSCRIPTION_SUPPORT,
        ))
    }

    fn start_sign_in(
        &self,
        host: Arc<dyn ModelHost>,
        request: SignInRequest,
    ) -> SignInFuture<Result<SignInStart, SignInError>> {
        Box::pin(async move {
            // The door recorded a start, so the attempt still gets its end.
            host.sign_in_settled(
                &request.sign_in_ref,
                request.provider,
                SignInOutcome::Failed {
                    kind: SignInFailureKind::Other,
                    account_id: request.target.account_id().map(ToString::to_string),
                },
            );
            Err(SignInError::Failed(format!(
                "Subscription sign-in is unavailable: {NO_SUBSCRIPTION_SUPPORT}."
            )))
        })
    }

    fn sign_in_status(&self, _sign_in_ref: &str) -> Result<SignInStatus, SignInError> {
        Err(no_pending_sign_in())
    }

    fn submit_sign_in_code(
        &self,
        _host: Arc<dyn ModelHost>,
        _sign_in_ref: String,
        _code: String,
    ) -> SignInFuture<Result<SubmitCodeResult, SignInError>> {
        Box::pin(async { Err(no_pending_sign_in()) })
    }

    fn cancel_sign_in(&self, _host: &dyn ModelHost, _sign_in_ref: &str) -> bool {
        false
    }

    fn pending_sign_ins(&self) -> Vec<PendingSignIn> {
        Vec::new()
    }
}

/// What every sign-in call says about a reference it does not hold.
pub fn no_pending_sign_in() -> SignInError {
    SignInError::BadRequest("That sign-in session has expired. Start signing in again.".to_string())
}

/// Test doubles for hosts that exercise account homes without the private
/// crate.
#[cfg(any(test, feature = "test-utils"))]
pub mod testing {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// Places managed Claude homes anywhere under `root`, and answers
    /// "signed in" from a marker file [`TempHomes::sign_in`] writes.
    ///
    /// Sign-in is scripted the way the real flows behave. A Claude sign-in
    /// parks pending (creating a provisional row for a new account) until a
    /// code arrives: one containing `#` completes it, as a `code#state` paste
    /// does, and any other is refused in place. A Codex sign-in parks pending
    /// until [`TempHomes::complete_codex`]. Each holds [`SIGN_IN_VERIFIER`],
    /// registered as a secret the way a PKCE verifier would be, so a test can
    /// prove no surface ever shows it. Every other subscription question gets
    /// [`Unsubscribed`]'s answer.
    #[derive(Debug, Clone)]
    pub struct TempHomes {
        pub root: PathBuf,
        sign_ins: Arc<Mutex<BTreeMap<String, ScriptedSignIn>>>,
        usage: Arc<Mutex<Option<ScriptedUsage>>>,
        generation: Arc<Mutex<Option<String>>>,
        probes: Arc<std::sync::atomic::AtomicUsize>,
    }

    /// A scripted probe's answer: the snapshot, and the credential generation
    /// it reports having read.
    type ScriptedUsage = (ProviderUsageSnapshot, Option<String>);

    #[derive(Debug, Clone)]
    struct ScriptedSignIn {
        provider: RoutedProvider,
        target: SignInTarget,
        account_id: Option<String>,
        provisional: bool,
        project_id: Option<String>,
        started_at: i64,
    }

    /// The stand-in PKCE verifier every scripted sign-in holds.
    pub const SIGN_IN_VERIFIER: &str = "pkce-verifier-never-shown";
    /// What a scripted Claude sign-in reports on completion.
    pub const SIGNED_IN_EMAIL: &str = "op@example.com";
    pub const SIGNED_IN_PLAN: &str = "max";

    const SIGNED_IN_MARKER: &str = ".signed-in";

    impl TempHomes {
        pub fn new(root: impl Into<PathBuf>) -> Self {
            Self {
                root: root.into(),
                sign_ins: Arc::default(),
                usage: Arc::default(),
                generation: Arc::default(),
                probes: Arc::default(),
            }
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

        /// The open URL a scripted sign-in hands back: the provider's own
        /// `state` and PKCE challenge, never the verifier or the sign-in's
        /// reference.
        pub fn open_url(provider: RoutedProvider) -> String {
            format!(
                "https://{}.invalid/sign-in?state=scripted-state&code_challenge=scripted-challenge",
                provider.backend()
            )
        }

        /// Answer every usage probe with `snapshot`, reporting it read
        /// credential `generation`.
        pub fn script_usage(&self, snapshot: ProviderUsageSnapshot, generation: Option<String>) {
            *self.usage.lock().unwrap() = Some((snapshot, generation));
        }

        /// The credential generation a probe of any account would read now.
        pub fn set_generation(&self, generation: Option<String>) {
            *self.generation.lock().unwrap() = generation;
        }

        /// How many usage probes have run.
        pub fn probe_count(&self) -> usize {
            self.probes.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// Finish a pending Codex sign-in as the browser would.
        pub fn complete_codex(
            &self,
            host: &dyn ModelHost,
            sign_in_ref: &str,
            auth_json: String,
        ) -> Result<AccountInfo, String> {
            let entry = self
                .sign_ins
                .lock()
                .unwrap()
                .remove(sign_in_ref)
                .ok_or("no pending Codex sign-in")?;
            let account =
                match host.record_codex_sign_in(auth_json, entry.project_id, &entry.target) {
                    Ok(account) => account,
                    Err(error) => {
                        host.sign_in_settled(
                            sign_in_ref,
                            RoutedProvider::Codex,
                            SignInOutcome::Failed {
                                kind: error.kind,
                                account_id: entry.target.account_id().map(ToString::to_string),
                            },
                        );
                        return Err(error.message);
                    }
                };
            host.sign_in_settled(
                sign_in_ref,
                RoutedProvider::Codex,
                SignInOutcome::Completed {
                    account_id: account.id.clone(),
                    email: None,
                    plan: None,
                },
            );
            Ok(account)
        }

        fn now() -> i64 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs() as i64)
                .unwrap_or_default()
        }
    }

    impl Subscriptions for TempHomes {
        fn claude_home(&self, _host: &dyn ModelHost, account_id: &str) -> Option<PathBuf> {
            Some(self.home(account_id))
        }

        fn prepare_claude_home(
            &self,
            _host: &dyn ModelHost,
            account_id: &str,
        ) -> Result<PathBuf, String> {
            let home = self.home(account_id);
            std::fs::create_dir_all(&home).map_err(|err| err.to_string())?;
            Ok(home)
        }

        fn claude_home_signed_in(&self, _host: &dyn ModelHost, account_id: &str) -> bool {
            self.home(account_id).join(SIGNED_IN_MARKER).exists()
        }

        fn release_claude_home(
            &self,
            _host: &dyn ModelHost,
            account_id: &str,
        ) -> Result<(), String> {
            let home = self.home(account_id);
            if home.exists() {
                std::fs::remove_dir_all(&home).map_err(|err| err.to_string())?;
            }
            Ok(())
        }

        /// The test home, presented the way a managed home is.
        fn claude_session_credential(
            &self,
            host: &dyn ModelHost,
            account_id: &str,
        ) -> Result<ClaudeSessionCredential, String> {
            let home = self.prepare_claude_home(host, account_id)?;
            Ok(ClaudeSessionCredential {
                env: SecretEnv::new().with("CLAUDE_CONFIG_DIR", home.to_string_lossy()),
                generation: 0,
            })
        }

        fn renew_claude_credentials(&self, host: &dyn ModelHost) -> Vec<String> {
            Unsubscribed.renew_claude_credentials(host)
        }

        fn claude_credential_rejected(
            &self,
            host: &dyn ModelHost,
            account_id: &str,
            generation: u64,
        ) -> Result<ClaudeSessionCredential, String> {
            Unsubscribed.claude_credential_rejected(host, account_id, generation)
        }

        fn migrate_claude_homes(&self, host: &dyn ModelHost) -> Vec<String> {
            Unsubscribed.migrate_claude_homes(host)
        }

        fn probe_usage(
            &self,
            host: &dyn ModelHost,
            provider: RoutedProvider,
            account_id: Option<&str>,
        ) -> (ProviderUsageSnapshot, Option<String>) {
            self.probes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match self.usage.lock().unwrap().clone() {
                Some(scripted) => scripted,
                None => Unsubscribed.probe_usage(host, provider, account_id),
            }
        }

        fn credential_generation(
            &self,
            _host: &dyn ModelHost,
            _provider: RoutedProvider,
            _account_id: &str,
        ) -> Option<String> {
            self.generation.lock().unwrap().clone()
        }

        fn consume_usage_reset(
            &self,
            host: &dyn ModelHost,
            provider: RoutedProvider,
            account_id: Option<&str>,
        ) -> Result<ProviderUsageResetResult, String> {
            Unsubscribed.consume_usage_reset(host, provider, account_id)
        }

        fn codex_session_login(
            &self,
            host: &dyn ModelHost,
            auth: &CodexAuthState,
        ) -> Result<Secret<Value>, String> {
            Unsubscribed.codex_session_login(host, auth)
        }

        fn refresh_codex(
            &self,
            host: &dyn ModelHost,
            request: CodexRefreshRequest,
        ) -> Result<(String, CodexAccountRef), CodexRefreshFailure> {
            Unsubscribed.refresh_codex(host, request)
        }

        fn start_sign_in(
            &self,
            host: Arc<dyn ModelHost>,
            request: SignInRequest,
        ) -> SignInFuture<Result<SignInStart, SignInError>> {
            let sign_ins = self.sign_ins.clone();
            Box::pin(async move {
                host.register_secret(
                    request.provider.backend(),
                    "sign_in_verifier",
                    SIGN_IN_VERIFIER,
                );
                let fail = |message: String| {
                    host.sign_in_settled(
                        &request.sign_in_ref,
                        request.provider,
                        SignInOutcome::Failed {
                            kind: SignInFailureKind::Other,
                            account_id: request.target.account_id().map(ToString::to_string),
                        },
                    );
                    Err(SignInError::Failed(message))
                };
                let (account_id, provisional) =
                    match (request.provider, request.target.account_id()) {
                        (RoutedProvider::Claude, Some(id)) => {
                            if !host.is_managed_claude_account(id, request.project_id.as_deref()) {
                                return fail(format!("Managed Claude account not found: {id}"));
                            }
                            (Some(id.to_string()), false)
                        }
                        (RoutedProvider::Claude, None) => {
                            match host.add_claude_profile_account(request.project_id.clone()) {
                                Ok(account) => (Some(account.id), true),
                                Err(error) => return fail(error),
                            }
                        }
                        (RoutedProvider::Codex, account_id) => {
                            (account_id.map(ToString::to_string), false)
                        }
                    };
                sign_ins.lock().unwrap().insert(
                    request.sign_in_ref.clone(),
                    ScriptedSignIn {
                        provider: request.provider,
                        target: request.target.clone(),
                        account_id: account_id.clone(),
                        provisional,
                        project_id: request.project_id.clone(),
                        started_at: Self::now(),
                    },
                );
                Ok(SignInStart::Pending {
                    open_url: Some(Self::open_url(request.provider)),
                    sign_in_ref: request.sign_in_ref,
                    account_id,
                })
            })
        }

        fn sign_in_status(&self, sign_in_ref: &str) -> Result<SignInStatus, SignInError> {
            if self.sign_ins.lock().unwrap().contains_key(sign_in_ref) {
                Ok(SignInStatus::Pending)
            } else {
                Err(no_pending_sign_in())
            }
        }

        fn submit_sign_in_code(
            &self,
            host: Arc<dyn ModelHost>,
            sign_in_ref: String,
            code: String,
        ) -> SignInFuture<Result<SubmitCodeResult, SignInError>> {
            let homes = self.clone();
            Box::pin(async move {
                host.register_secret("claude", "sign_in_code", &code);
                let mut sign_ins = homes.sign_ins.lock().unwrap();
                let Some(entry) = sign_ins
                    .get(&sign_in_ref)
                    .filter(|entry| entry.provider == RoutedProvider::Claude)
                    .cloned()
                else {
                    return Err(no_pending_sign_in());
                };
                if !code.contains('#') {
                    return Ok(SubmitCodeResult::InvalidCode {
                        message: "Invalid code. Please make sure the full code was copied."
                            .to_string(),
                    });
                }
                sign_ins.remove(&sign_in_ref);
                drop(sign_ins);
                let account_id = entry.account_id.expect("a Claude sign-in has a row");
                homes.sign_in(&account_id);
                let account = host
                    .record_claude_sign_in(
                        &account_id,
                        Some(SIGNED_IN_EMAIL.to_string()),
                        Some(SIGNED_IN_PLAN.to_string()),
                    )
                    .map_err(SignInError::Failed)?;
                host.sign_in_settled(
                    &sign_in_ref,
                    RoutedProvider::Claude,
                    SignInOutcome::Completed {
                        account_id,
                        email: Some(SIGNED_IN_EMAIL.to_string()),
                        plan: Some(SIGNED_IN_PLAN.to_string()),
                    },
                );
                Ok(SubmitCodeResult::Connected {
                    account: Box::new(account),
                })
            })
        }

        fn cancel_sign_in(&self, host: &dyn ModelHost, sign_in_ref: &str) -> bool {
            let Some(entry) = self.sign_ins.lock().unwrap().remove(sign_in_ref) else {
                return false;
            };
            if entry.provisional {
                if let Some(id) = &entry.account_id {
                    host.remove_account(id);
                }
            }
            host.sign_in_settled(
                sign_in_ref,
                entry.provider,
                SignInOutcome::Cancelled {
                    account_id: entry.account_id.filter(|_| !entry.provisional),
                },
            );
            true
        }

        fn pending_sign_ins(&self) -> Vec<PendingSignIn> {
            let mut pending: Vec<PendingSignIn> = self
                .sign_ins
                .lock()
                .unwrap()
                .iter()
                .map(|(sign_in_ref, entry)| PendingSignIn {
                    sign_in_ref: sign_in_ref.clone(),
                    provider: entry.provider,
                    account_id: entry.account_id.clone(),
                    started_at: entry.started_at,
                })
                .collect();
            pending.sort_by_key(|entry| entry.started_at);
            pending
        }
    }
}
