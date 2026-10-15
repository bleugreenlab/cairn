//! Keeping live Claude processes on their account's current credential.
//!
//! A managed Claude process runs on an access token alone; Cairn's refresh
//! authority (subscription mechanics, behind `Subscriptions`) is the only
//! spender of the account's refresh token. When it publishes a new generation,
//! every live process on that account has to be handed the new token, or it
//! runs on until its own token expires and a request is refused.
//!
//! The rule is keyed to the store, not to who renewed: a process is pushed
//! whenever the account's current generation is newer than the one it was
//! last pushed. Reconciling runs when a credential is published in this
//! process (a launch, a sweep, a 401, a usage probe), on every process sweep
//! (which also catches a rotation another Cairn process published), and after
//! a 401. The CLI acknowledges each push; a 401 that arrives anyway is
//! answered with the current (or a renewed) token while the CLI waits for it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use cairn_models::claude::tokens::{stored_claude_oauth, ClaudeOAuth, ClaudeSessionCredential};
use cairn_models::subscription::ModelHost;
use cairn_session::agent_process::process::FedProcess;

use super::Orchestrator;

/// One sweep's renewal at a time: a renewal can wait on a provider round
/// trip and a lease, and must never pile up behind the 30-second sweep.
static RENEWAL_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl Orchestrator {
    /// The account's credential as it stands on disk.
    fn durable_claude_oauth(&self, account_id: &str) -> Option<ClaudeOAuth> {
        let store = ModelHost::store(self).durable()?;
        let account = store.accounts.iter().find(|a| a.id == account_id)?;
        stored_claude_oauth(account)?.ok()
    }

    /// Push the current credential to every live process (on `account_id`,
    /// or on any account) that has not been pushed it yet.
    pub(crate) fn reconcile_claude_credentials(&self, account_id: Option<&str>) {
        let fed = self.process_state.fed_processes(account_id);
        if fed.is_empty() {
            return;
        }
        let Some(store) = ModelHost::store(self).durable() else {
            return;
        };
        for process in fed {
            let current = store
                .accounts
                .iter()
                .find(|account| account.id == process.account_id)
                .and_then(stored_claude_oauth)
                .and_then(Result::ok);
            let Some(current) = current else {
                continue;
            };
            push(&process, &ClaudeSessionCredential::for_push(&current));
        }
    }

    /// A live process on a managed account was refused with a 401. Answer it
    /// with the credential it should hold now, and bring every other process
    /// on the account along.
    pub(crate) fn claude_credential_rejected(&self, run_id: &str) {
        let Some(process) = self.process_state.fed_process(run_id) else {
            return;
        };
        let held = process.feed.acknowledged();
        if let Some(current) = self.durable_claude_oauth(&process.account_id) {
            if current.generation() > held {
                // The production answer to whether a refresh revokes the
                // previous access token at once.
                log::info!(
                    "Claude account {}: run {} was refused on superseded generation {held}, {} ms after generation {} was published",
                    process.account_id,
                    &run_id[..run_id.len().min(8)],
                    current
                        .published_at_ms()
                        .map_or(-1, |at| now_ms() - at),
                    current.generation()
                );
            }
        }
        match self
            .subscriptions()
            .claude_credential_rejected(self, &process.account_id, held)
        {
            Ok(credential) if credential.generation > held => {
                push(&process, &credential);
                self.reconcile_claude_credentials(Some(&process.account_id));
            }
            Ok(_) => log::warn!(
                "Claude account {}: run {} was refused on its current credential and there is no newer one to give it",
                process.account_id,
                &run_id[..run_id.len().min(8)]
            ),
            Err(error) => log::warn!("{error}"),
        }
    }

    /// A live process confirmed it now holds `generation`.
    pub(crate) fn claude_credential_acknowledged(&self, run_id: &str, generation: u64) {
        if let Some(process) = self.process_state.fed_process(run_id) {
            process.feed.record_ack(generation);
            log::info!(
                "Claude account {}: run {} acknowledged credential generation {generation}",
                process.account_id,
                &run_id[..run_id.len().min(8)]
            );
        }
    }

    /// Renew managed Claude credentials close to expiry, then bring every
    /// live process up to date. Blocking; skipped while a previous sweep's
    /// renewal is still in flight.
    pub(crate) fn sweep_claude_credentials(&self) {
        if RENEWAL_IN_FLIGHT.swap(true, Ordering::AcqRel) {
            return;
        }
        let advanced = self.subscriptions().renew_claude_credentials(self);
        if !advanced.is_empty() {
            log::info!("Renewed Claude credentials for {}", advanced.join(", "));
        }
        self.reconcile_claude_credentials(None);
        RENEWAL_IN_FLIGHT.store(false, Ordering::Release);
    }

    /// A credential was published in this process: push it wherever it is
    /// behind, off the publishing thread.
    pub(crate) fn claude_credentials_published(&self) {
        if self.process_state.fed_processes(None).is_empty() {
            return;
        }
        let orch = Arc::new(self.clone());
        std::thread::spawn(move || orch.reconcile_claude_credentials(None));
    }

    /// Take over every managed Claude home signed in before Cairn held its
    /// tokens. Once, at startup, before sessions launch on them.
    pub(crate) fn migrate_claude_homes(&self) {
        let migrated = self.subscriptions().migrate_claude_homes(self);
        if !migrated.is_empty() {
            log::info!(
                "Took over the Claude credentials of {} managed account(s): {}",
                migrated.len(),
                migrated.join(", ")
            );
        }
    }
}

fn push(process: &FedProcess, credential: &ClaudeSessionCredential) {
    match process.push(&credential.env, credential.generation) {
        Ok(false) => {}
        Ok(true) => log::info!(
            "Claude account {}: pushed credential generation {} to run {}",
            process.account_id,
            credential.generation,
            &process.run_id[..process.run_id.len().min(8)]
        ),
        // A process whose pipe is gone is exiting; the next launch is fed the
        // current credential.
        Err(error) => log::info!(
            "Claude account {}: run {} did not take credential generation {}: {error}",
            process.account_id,
            &process.run_id[..process.run_id.len().min(8)],
            credential.generation
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use cairn_models::accounts::{ApiProvider, ProviderAuth};
    use cairn_models::interface::session::{SecretEnv, SessionControl, SessionFacts, SessionInput};
    use cairn_models::subscription::{Subscriptions, Unsubscribed};
    use cairn_models::wire::MessageContent;
    use cairn_session::agent_process::process::{CredentialFeed, RunHandle};

    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};

    const ACCOUNT: &str = "acc_claude";
    const OTHER: &str = "acc_other";
    const HOUR_MS: i64 = 60 * 60 * 1000;

    /// Every `(generation, token)` pushed into one fake process.
    type Pushes = Arc<Mutex<Vec<(u64, String)>>>;

    struct RecordingControl(Pushes);

    impl SessionControl for RecordingControl {
        fn open_turn(&mut self, _content: &MessageContent) -> Result<(), String> {
            Ok(())
        }
        fn send(&mut self, _input: &SessionInput<'_>) -> Result<(), String> {
            Ok(())
        }
        fn interrupt(&mut self) -> Result<(), String> {
            Ok(())
        }
        fn set_model(&mut self, _model: &str) -> Result<(), String> {
            Ok(())
        }
        fn set_permission_mode(&mut self, _mode: &str) -> Result<(), String> {
            Ok(())
        }
        fn update_credential(&mut self, env: &SecretEnv, generation: u64) -> Result<(), String> {
            // Only the token travels into a live process.
            assert_eq!(
                env.names().collect::<Vec<_>>(),
                vec![cairn_models::claude::tokens::OAUTH_TOKEN_ENV]
            );
            let expected = format!("at-{generation}");
            let token = if env.contains_value(&expected) {
                expected
            } else {
                "<a token from another generation>".to_string()
            };
            self.0.lock().unwrap().push((generation, token));
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    /// Stands in for the refresh authority: every renewal publishes the next
    /// generation (token `at-<generation>`) and announces it, the way the real
    /// one does. Accounts in `due` are what a sweep finds close to expiry.
    struct Rotator {
        due: Vec<String>,
    }

    fn rotate(host: &dyn ModelHost, account_id: &str) -> ClaudeOAuth {
        let next = host
            .store()
            .update(|store| {
                let account = store
                    .accounts
                    .iter_mut()
                    .find(|account| account.id == account_id)
                    .ok_or("no such account")?;
                let current = stored_claude_oauth(account).ok_or("not harvested")??;
                let generation = current.generation() + 1;
                let next = current.rotated(
                    &format!("at-{generation}"),
                    None,
                    now_ms() + 8 * HOUR_MS,
                    None,
                    now_ms(),
                );
                account.auth = ProviderAuth::ClaudeProfile {
                    oauth: Some(next.to_json()),
                };
                Ok(next)
            })
            .unwrap();
        host.credentials_published();
        next
    }

    impl Subscriptions for Rotator {
        fn claude_home(&self, host: &dyn ModelHost, id: &str) -> Option<std::path::PathBuf> {
            Unsubscribed.claude_home(host, id)
        }
        fn prepare_claude_home(
            &self,
            host: &dyn ModelHost,
            id: &str,
        ) -> Result<std::path::PathBuf, String> {
            Unsubscribed.prepare_claude_home(host, id)
        }
        fn claude_home_signed_in(&self, host: &dyn ModelHost, id: &str) -> bool {
            Unsubscribed.claude_home_signed_in(host, id)
        }
        fn release_claude_home(&self, host: &dyn ModelHost, id: &str) -> Result<(), String> {
            Unsubscribed.release_claude_home(host, id)
        }
        /// A launch below the floor renews first.
        fn claude_session_credential(
            &self,
            host: &dyn ModelHost,
            id: &str,
        ) -> Result<ClaudeSessionCredential, String> {
            Ok(ClaudeSessionCredential::for_push(&rotate(host, id)))
        }
        fn renew_claude_credentials(&self, host: &dyn ModelHost) -> Vec<String> {
            for id in &self.due {
                rotate(host, id);
            }
            self.due.clone()
        }
        fn claude_credential_rejected(
            &self,
            host: &dyn ModelHost,
            id: &str,
            generation: u64,
        ) -> Result<ClaudeSessionCredential, String> {
            let store = host.store().durable().unwrap();
            let account = store.accounts.iter().find(|a| a.id == id).unwrap();
            let current = stored_claude_oauth(account).unwrap()?;
            let current = if current.generation() == generation {
                rotate(host, id)
            } else {
                current
            };
            Ok(ClaudeSessionCredential::for_push(&current))
        }
        fn migrate_claude_homes(&self, host: &dyn ModelHost) -> Vec<String> {
            Unsubscribed.migrate_claude_homes(host)
        }
        fn codex_session_login(
            &self,
            host: &dyn ModelHost,
            auth: &cairn_models::codex::tokens::CodexAuthState,
        ) -> Result<cairn_models::interface::session::Secret<serde_json::Value>, String> {
            Unsubscribed.codex_session_login(host, auth)
        }
        fn probe_usage(
            &self,
            host: &dyn ModelHost,
            provider: cairn_models::accounts::RoutedProvider,
            id: Option<&str>,
        ) -> (
            cairn_models::types::usage::ProviderUsageSnapshot,
            Option<String>,
        ) {
            Unsubscribed.probe_usage(host, provider, id)
        }
        fn credential_generation(
            &self,
            host: &dyn ModelHost,
            provider: cairn_models::accounts::RoutedProvider,
            id: &str,
        ) -> Option<String> {
            Unsubscribed.credential_generation(host, provider, id)
        }
        fn consume_usage_reset(
            &self,
            host: &dyn ModelHost,
            provider: cairn_models::accounts::RoutedProvider,
            id: Option<&str>,
        ) -> Result<cairn_models::types::usage::ProviderUsageResetResult, String> {
            Unsubscribed.consume_usage_reset(host, provider, id)
        }
        fn refresh_codex(
            &self,
            host: &dyn ModelHost,
            request: cairn_models::codex::tokens::CodexRefreshRequest,
        ) -> Result<
            (String, cairn_models::codex::tokens::CodexAccountRef),
            cairn_models::codex::tokens::CodexRefreshFailure,
        > {
            Unsubscribed.refresh_codex(host, request)
        }

        fn start_sign_in(
            &self,
            host: std::sync::Arc<dyn ModelHost>,
            request: cairn_models::subscription::SignInRequest,
        ) -> cairn_models::subscription::SignInFuture<
            Result<
                cairn_models::subscription::SignInStart,
                cairn_models::subscription::SignInError,
            >,
        > {
            Unsubscribed.start_sign_in(host, request)
        }
        fn sign_in_status(
            &self,
            sign_in_ref: &str,
        ) -> Result<cairn_models::subscription::SignInStatus, cairn_models::subscription::SignInError>
        {
            Unsubscribed.sign_in_status(sign_in_ref)
        }
        fn submit_sign_in_code(
            &self,
            host: std::sync::Arc<dyn ModelHost>,
            sign_in_ref: String,
            code: String,
        ) -> cairn_models::subscription::SignInFuture<
            Result<
                cairn_models::subscription::SubmitCodeResult,
                cairn_models::subscription::SignInError,
            >,
        > {
            Unsubscribed.submit_sign_in_code(host, sign_in_ref, code)
        }
        fn cancel_sign_in(&self, host: &dyn ModelHost, sign_in_ref: &str) -> bool {
            Unsubscribed.cancel_sign_in(host, sign_in_ref)
        }
        fn pending_sign_ins(&self) -> Vec<cairn_models::subscription::PendingSignIn> {
            Unsubscribed.pending_sign_ins()
        }
    }

    async fn orchestrator(due: &[&str]) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(db_state, services, config_dir)
            .subscriptions(Arc::new(Rotator {
                due: due.iter().map(|id| id.to_string()).collect(),
            }))
            .build();
        let first = ClaudeOAuth::parse(
            &serde_json::json!({
                "accessToken": "at-1",
                "refreshToken": "rt-1",
                "expiresAt": now_ms() + 8 * HOUR_MS,
            })
            .to_string(),
        )
        .unwrap()
        .stamped(1, now_ms());
        // Added the ordinary way (which also creates the store), then given
        // the fixed ids the fake's `due` list names.
        let added: Vec<String> = [ACCOUNT, OTHER]
            .iter()
            .map(|label| {
                orch.add_account(
                    ApiProvider::Anthropic,
                    label.to_string(),
                    ProviderAuth::ClaudeProfile {
                        oauth: Some(first.to_json()),
                    },
                    None,
                )
                .unwrap()
                .id
            })
            .collect();
        ModelHost::store(&orch)
            .update(|store| {
                for (generated, id) in added.iter().zip([ACCOUNT, OTHER]) {
                    let account = store
                        .accounts
                        .iter_mut()
                        .find(|account| &account.id == generated)
                        .ok_or("the added account")?;
                    account.id = id.to_string();
                }
                Ok(())
            })
            .unwrap();
        orch
    }

    /// A live Claude process on `account_id`, launched on generation 1.
    fn live(orch: &Orchestrator, run_id: &str, account_id: &str) -> (Pushes, Arc<CredentialFeed>) {
        let pushes = Pushes::default();
        let control: Box<dyn SessionControl> = Box::new(RecordingControl(pushes.clone()));
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(control))),
            Some(format!("session-{run_id}")),
            None,
        );
        handle.facts = Some(SessionFacts {
            provider: "claude".into(),
            backend_session_id: None,
            account_id: Some(account_id.into()),
            host: "local".into(),
            model: None,
        });
        let feed = Arc::new(CredentialFeed::launched_on(1));
        handle.credential_feed = Some(feed.clone());
        orch.process_state
            .register_process(run_id.into(), handle)
            .unwrap();
        (pushes, feed)
    }

    fn wait_for(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn pushed(pushes: &Pushes) -> Vec<(u64, String)> {
        pushes.lock().unwrap().clone()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_launch_triggered_renewal_pushes_to_an_already_live_session() {
        let orch = orchestrator(&[]).await;
        let (pushes, feed) = live(&orch, "run-live", ACCOUNT);

        // A second launch on the account renews below the launch floor.
        let launched = orch
            .subscriptions()
            .claude_session_credential(&orch, ACCOUNT)
            .unwrap();
        assert_eq!(launched.generation, 2);

        wait_for("the push", || !pushed(&pushes).is_empty());
        assert_eq!(pushed(&pushes), vec![(2, "at-2".to_string())]);
        assert_eq!(feed.pushed(), 2);
        assert_eq!(feed.acknowledged(), 1, "not held until the CLI says so");

        orch.claude_credential_acknowledged("run-live", 2);
        assert_eq!(feed.acknowledged(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sweep_pushes_every_live_process_on_a_renewed_account_once_and_no_other() {
        let orch = orchestrator(&[ACCOUNT]).await;
        let (first, _) = live(&orch, "run-a", ACCOUNT);
        let (second, _) = live(&orch, "run-b", ACCOUNT);
        let (other, other_feed) = live(&orch, "run-c", OTHER);

        orch.sweep_claude_credentials();
        // The publish also reconciles from its own thread; let it run out.
        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(pushed(&first), vec![(2, "at-2".to_string())]);
        assert_eq!(pushed(&second), vec![(2, "at-2".to_string())]);
        assert!(pushed(&other).is_empty());
        assert_eq!(other_feed.pushed(), 1);

        // The next renewal is pushed once too, and the one before is not
        // repeated.
        orch.sweep_claude_credentials();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            pushed(&first),
            vec![(2, "at-2".to_string()), (3, "at-3".to_string())]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_401_on_the_current_generation_renews_and_pushes_the_account() {
        let orch = orchestrator(&[]).await;
        let (rejected, _) = live(&orch, "run-rejected", ACCOUNT);
        let (sibling, _) = live(&orch, "run-sibling", ACCOUNT);

        orch.claude_credential_rejected("run-rejected");
        wait_for("the sibling push", || !pushed(&sibling).is_empty());

        assert_eq!(pushed(&rejected), vec![(2, "at-2".to_string())]);
        assert_eq!(pushed(&sibling), vec![(2, "at-2".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_401_on_a_superseded_generation_is_answered_without_renewing() {
        let orch = orchestrator(&[]).await;
        let (pushes, feed) = live(&orch, "run-behind", ACCOUNT);
        // Rotated elsewhere (another Cairn process) without a push reaching
        // this one: no publish happened here.
        ModelHost::store(&orch)
            .update(|store| {
                let account = store.accounts.iter_mut().find(|a| a.id == ACCOUNT).unwrap();
                let next = stored_claude_oauth(account).unwrap()?.rotated(
                    "at-2",
                    None,
                    now_ms() + 8 * HOUR_MS,
                    None,
                    now_ms(),
                );
                account.auth = ProviderAuth::ClaudeProfile {
                    oauth: Some(next.to_json()),
                };
                Ok(())
            })
            .unwrap();

        orch.claude_credential_rejected("run-behind");

        assert_eq!(pushed(&pushes), vec![(2, "at-2".to_string())]);
        assert_eq!(feed.pushed(), 2);
        let store = ModelHost::store(&orch).durable().unwrap();
        let account = store.accounts.iter().find(|a| a.id == ACCOUNT).unwrap();
        assert_eq!(
            stored_claude_oauth(account).unwrap().unwrap().generation(),
            2,
            "nothing was spent"
        );
    }
}
