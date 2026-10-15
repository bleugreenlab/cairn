//! Keeping live Claude sessions on their account's current credential.
//!
//! A managed Claude session runs on an access token alone; the host's refresh
//! authority (subscription mechanics, behind [`Subscriptions`]) is the only
//! spender of the account's refresh token. When it publishes a new generation,
//! every live session on that account has to be handed the new token, or it
//! runs on until its own token expires and a request is refused.
//!
//! The rule is keyed to the store, not to who renewed: a session is pushed
//! whenever the account's current generation is newer than the one it was
//! last pushed. Reconciling runs when a credential is published in this
//! process (a launch, a sweep, a 401, a usage probe), on every sweep (which
//! also catches a rotation another process sharing the store published), and
//! after a 401. The CLI acknowledges each push; a 401 that arrives anyway is
//! answered with the current (or a renewed) token while the CLI waits for it.
//!
//! [`ClaudeCredentialFeeds`] is that rule, for Cairn's runtime and for any
//! host built on the facet alike. It holds no subscription mechanics: it reads
//! the store, and asks [`Subscriptions`] for every renewal. What a host owes
//! it, once launch has handed a session its first credential (which lasts at
//! least the launch floor, and which the CLI never renews itself):
//!
//! 1. [`ClaudeCredentialFeeds::migrate_homes`] once at start, before sessions
//!    launch.
//! 2. [`ClaudeCredentialFeeds::register`] each interactive session at launch,
//!    with the generation its launch credential carried, and hold the
//!    [`FeedRegistration`] for as long as the session lives. Dropping it
//!    deregisters the session.
//! 3. [`ClaudeCredentialFeeds::observe`] every [`SessionEvent`] the session
//!    emits, off any async executor: a rejection blocks on a renewal.
//! 4. [`ClaudeCredentialFeeds::published`] from
//!    [`ModelHost::credentials_published`].
//! 5. [`ClaudeCredentialFeeds::sweep`] every [`SWEEP_INTERVAL`], on a thread
//!    that may block.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::accounts::CredentialStoreHandle;
use crate::claude::tokens::{stored_claude_oauth, ClaudeOAuth, ClaudeSessionCredential};
use crate::interface::session::{SecretEnv, SharedControl, NO_SESSION_CONTROL};
use crate::session::events::SessionEvent;
use crate::subscription::{ModelHost, Subscriptions};

/// How often a host sweeps. A sweep renews nothing until an account nears
/// expiry, so most are a store read and nothing else.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn short(id: &str) -> &str {
    &id[..id.len().min(8)]
}

/// Which rotation of its account's credential a live session holds.
///
/// A session launched on a managed subscription runs on an access token its
/// host rotates underneath it; each rotation is pushed in over the session's
/// control. `pushed` is the newest generation sent, `acknowledged` the newest
/// the session confirmed, and a session is behind while either trails the
/// store's current generation.
#[derive(Debug)]
pub struct CredentialFeed {
    pushed: AtomicU64,
    acknowledged: AtomicU64,
}

impl CredentialFeed {
    /// A session launched on `generation`.
    pub fn launched_on(generation: u64) -> Self {
        Self {
            pushed: AtomicU64::new(generation),
            acknowledged: AtomicU64::new(generation),
        }
    }

    pub fn pushed(&self) -> u64 {
        self.pushed.load(Ordering::Acquire)
    }

    pub fn acknowledged(&self) -> u64 {
        self.acknowledged.load(Ordering::Acquire)
    }

    pub fn record_push(&self, generation: u64) {
        self.pushed.fetch_max(generation, Ordering::AcqRel);
    }

    pub fn record_ack(&self, generation: u64) {
        self.acknowledged.fetch_max(generation, Ordering::AcqRel);
        self.record_push(generation);
    }
}

/// One live session fed an account's credential, and how to reach it.
struct FedSession {
    session_id: String,
    account_id: String,
    feed: CredentialFeed,
    control: SharedControl,
}

impl FedSession {
    /// Hand the session `generation` of its credential, unless it has already
    /// been pushed that generation or a newer one. Answers whether anything
    /// was sent.
    ///
    /// The check, the send and the record all happen under the session's
    /// control lock, so concurrent reconcilers (a sweep, a publish, a 401)
    /// push each generation to a session once, and a send that failed claims
    /// nothing.
    fn push(&self, env: &SecretEnv, generation: u64) -> Result<bool, String> {
        let mut control = self.control.lock().map_err(|e| e.to_string())?;
        if self.feed.pushed() >= generation {
            return Ok(false);
        }
        let control = control
            .as_mut()
            .ok_or_else(|| NO_SESSION_CONTROL.to_string())?;
        control.update_credential(env, generation)?;
        self.feed.record_push(generation);
        Ok(true)
    }

    fn deliver(&self, credential: &ClaudeSessionCredential) {
        match self.push(&credential.env, credential.generation) {
            Ok(false) => {}
            Ok(true) => log::info!(
                "Claude account {}: pushed credential generation {} to run {}",
                self.account_id,
                credential.generation,
                short(&self.session_id)
            ),
            // A session whose pipe is gone is exiting; the next launch is fed
            // the current credential.
            Err(error) => log::info!(
                "Claude account {}: run {} did not take credential generation {}: {error}",
                self.account_id,
                short(&self.session_id),
                credential.generation
            ),
        }
    }
}

struct Inner {
    store: CredentialStoreHandle,
    sessions: Mutex<HashMap<String, Arc<FedSession>>>,
    /// One sweep's renewal at a time: a renewal can wait on a provider round
    /// trip and a lease, and must never pile up behind the sweep interval.
    renewal_in_flight: AtomicBool,
}

/// Every live Claude session a host feeds, and the rule that keeps each on
/// its account's current credential. See the module documentation for what
/// a host owes it.
///
/// Clones share one registry. It holds a clone of the host's store and never
/// the host itself; the operations that may renew take the host and its
/// [`Subscriptions`] per call.
#[derive(Clone)]
pub struct ClaudeCredentialFeeds {
    inner: Arc<Inner>,
}

impl ClaudeCredentialFeeds {
    /// A coordinator reading `store`, the same store the host's
    /// [`ModelHost::store`] answers with.
    pub fn new(store: CredentialStoreHandle) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                sessions: Mutex::new(HashMap::new()),
                renewal_in_flight: AtomicBool::new(false),
            }),
        }
    }

    /// Feed a live session on `account_id`, launched on `launched_generation`,
    /// through `control`. Only an interactive session can take a pushed
    /// credential; a one-shot runs out its turn on the one it launched with,
    /// so it is not registered.
    ///
    /// The session is fed until the returned registration drops. Registering
    /// an id again replaces the earlier entry.
    pub fn register(
        &self,
        session_id: impl Into<String>,
        account_id: impl Into<String>,
        launched_generation: u64,
        control: SharedControl,
    ) -> FeedRegistration {
        let session = Arc::new(FedSession {
            session_id: session_id.into(),
            account_id: account_id.into(),
            feed: CredentialFeed::launched_on(launched_generation),
            control,
        });
        if let Ok(mut sessions) = self.inner.sessions.lock() {
            sessions.insert(session.session_id.clone(), session.clone());
        }
        FeedRegistration {
            feeds: Arc::downgrade(&self.inner),
            session,
        }
    }

    /// Act on one of `session_id`'s events: the two credential events are
    /// answered, every other event is ignored, so a host can hand over
    /// everything its reader sees.
    ///
    /// Blocks on a rejection until the session has been answered, which can
    /// mean waiting on a renewal's provider round trip. Never call this on an
    /// async executor's thread; use a thread of its own or a blocking pool.
    /// The CLI is waiting for the token meanwhile, and a Claude stream's lines
    /// are buffered by [`crate::session::events::EventReader`]'s own thread.
    pub fn observe(
        &self,
        host: &dyn ModelHost,
        subscriptions: &dyn Subscriptions,
        session_id: &str,
        event: &SessionEvent,
    ) {
        match event {
            SessionEvent::CredentialRejected => self.rejected(host, subscriptions, session_id),
            SessionEvent::CredentialAcknowledged { generation } => {
                self.acknowledged(session_id, *generation)
            }
            _ => {}
        }
    }

    /// Every fed session, optionally only those on `account_id`. Taken under
    /// the registry lock and released before anything is pushed.
    fn fed(&self, account_id: Option<&str>) -> Vec<Arc<FedSession>> {
        let Ok(sessions) = self.inner.sessions.lock() else {
            return Vec::new();
        };
        sessions
            .values()
            .filter(|session| !account_id.is_some_and(|wanted| wanted != session.account_id))
            .cloned()
            .collect()
    }

    fn session(&self, session_id: &str) -> Option<Arc<FedSession>> {
        self.inner.sessions.lock().ok()?.get(session_id).cloned()
    }

    /// The account's credential as it stands on disk.
    fn durable_claude_oauth(&self, account_id: &str) -> Option<ClaudeOAuth> {
        let store = self.inner.store.durable()?;
        let account = store.accounts.iter().find(|a| a.id == account_id)?;
        stored_claude_oauth(account)?.ok()
    }

    /// Push the current credential to every fed session (on `account_id`, or
    /// on any account) that has not been pushed it yet.
    pub fn reconcile(&self, account_id: Option<&str>) {
        let fed = self.fed(account_id);
        if fed.is_empty() {
            return;
        }
        let Some(store) = self.inner.store.durable() else {
            return;
        };
        for session in fed {
            let current = store
                .accounts
                .iter()
                .find(|account| account.id == session.account_id)
                .and_then(stored_claude_oauth)
                .and_then(Result::ok);
            let Some(current) = current else {
                continue;
            };
            session.deliver(&ClaudeSessionCredential::for_push(&current));
        }
    }

    /// A fed session was refused with a 401. Answer it with the credential
    /// it should hold now, and bring every other session on the account
    /// along. Blocking.
    pub fn rejected(
        &self,
        host: &dyn ModelHost,
        subscriptions: &dyn Subscriptions,
        session_id: &str,
    ) {
        let Some(session) = self.session(session_id) else {
            return;
        };
        let held = session.feed.acknowledged();
        if let Some(current) = self.durable_claude_oauth(&session.account_id) {
            if current.generation() > held {
                // The production answer to whether a refresh revokes the
                // previous access token at once.
                log::info!(
                    "Claude account {}: run {} was refused on superseded generation {held}, {} ms after generation {} was published",
                    session.account_id,
                    short(session_id),
                    current
                        .published_at_ms()
                        .map_or(-1, |at| now_ms() - at),
                    current.generation()
                );
            }
        }
        match subscriptions.claude_credential_rejected(host, &session.account_id, held) {
            Ok(credential) if credential.generation > held => {
                session.deliver(&credential);
                self.reconcile(Some(&session.account_id));
            }
            Ok(_) => log::warn!(
                "Claude account {}: run {} was refused on its current credential and there is no newer one to give it",
                session.account_id,
                short(session_id)
            ),
            Err(error) => log::warn!("{error}"),
        }
    }

    /// A fed session confirmed it now holds `generation`.
    pub fn acknowledged(&self, session_id: &str, generation: u64) {
        if let Some(session) = self.session(session_id) {
            session.feed.record_ack(generation);
            log::info!(
                "Claude account {}: run {} acknowledged credential generation {generation}",
                session.account_id,
                short(session_id)
            );
        }
    }

    /// Renew managed Claude credentials close to expiry, then bring every
    /// fed session up to date. Blocking; skipped while a previous sweep's
    /// renewal is still in flight.
    pub fn sweep(&self, host: &dyn ModelHost, subscriptions: &dyn Subscriptions) {
        if self.inner.renewal_in_flight.swap(true, Ordering::AcqRel) {
            return;
        }
        // Cleared however the renewal ends, a panic included, so one bad
        // sweep never stops every later one.
        struct Clear<'a>(&'a AtomicBool);
        impl Drop for Clear<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _clear = Clear(&self.inner.renewal_in_flight);
        let advanced = subscriptions.renew_claude_credentials(host);
        if !advanced.is_empty() {
            log::info!("Renewed Claude credentials for {}", advanced.join(", "));
        }
        self.reconcile(None);
    }

    /// A credential was published in this process: push it wherever it is
    /// behind, off the publishing thread. Never blocks.
    pub fn published(&self) {
        if self.fed(None).is_empty() {
            return;
        }
        let feeds = self.clone();
        std::thread::spawn(move || feeds.reconcile(None));
    }

    /// Take over every managed Claude home signed in before the host held
    /// its tokens. Once, at startup, before sessions launch on them.
    pub fn migrate_homes(&self, host: &dyn ModelHost, subscriptions: &dyn Subscriptions) {
        let migrated = subscriptions.migrate_claude_homes(host);
        if !migrated.is_empty() {
            log::info!(
                "Took over the Claude credentials of {} managed account(s): {}",
                migrated.len(),
                migrated.join(", ")
            );
        }
    }
}

/// A session's place in [`ClaudeCredentialFeeds`]. Dropping it stops the
/// session being fed.
pub struct FeedRegistration {
    feeds: Weak<Inner>,
    session: Arc<FedSession>,
}

impl FeedRegistration {
    /// Which generation the session was pushed, and which it confirmed.
    pub fn feed(&self) -> &CredentialFeed {
        &self.session.feed
    }
}

impl std::fmt::Debug for FeedRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedRegistration")
            .field("session_id", &self.session.session_id)
            .field("account_id", &self.session.account_id)
            .field("feed", &self.session.feed)
            .finish()
    }
}

impl Drop for FeedRegistration {
    /// Removes the entry only while it is still this registration's, so a
    /// displaced registration dropped after its id was registered again
    /// never removes its successor.
    fn drop(&mut self) {
        let Some(inner) = self.feeds.upgrade() else {
            return;
        };
        let Ok(mut sessions) = inner.sessions.lock() else {
            return;
        };
        if sessions
            .get(&self.session.session_id)
            .is_some_and(|current| Arc::ptr_eq(current, &self.session))
        {
            sessions.remove(&self.session.session_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    use cairn_platform::services::process::{ProcessSpawner, RealProcessSpawner};

    use crate::accounts::{
        AccountInfo, ApiProvider, CredentialStore, ProviderAuth, RoutedProvider,
    };
    use crate::interface::session::{SessionControl, SessionInput};
    use crate::subscription::testing::RotatingClaude;
    use crate::subscription::{SignInOutcome, SignInRecordError, SignInTarget};
    use crate::wire::MessageContent;

    const ACCOUNT: &str = "acc_claude";
    const OTHER: &str = "acc_other";
    const HOUR_MS: i64 = 60 * 60 * 1000;

    /// Every `(generation, token)` pushed into one fake session.
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
            // Only the token travels into a live session.
            assert_eq!(
                env.names().collect::<Vec<_>>(),
                vec![crate::claude::tokens::OAUTH_TOKEN_ENV]
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

    /// The least a host is: a store, and the coordinator told of every
    /// publish.
    struct Host {
        store: CredentialStoreHandle,
        feeds: ClaudeCredentialFeeds,
        spawner: RealProcessSpawner,
        _root: tempfile::TempDir,
    }

    impl ModelHost for Host {
        fn store(&self) -> &CredentialStoreHandle {
            &self.store
        }
        fn process_spawner(&self) -> &dyn ProcessSpawner {
            &self.spawner
        }
        fn claude_path(&self) -> Result<String, String> {
            Err("no CLI in this test".into())
        }
        fn codex_path(&self) -> Result<String, String> {
            Err("no CLI in this test".into())
        }
        fn credentials_published(&self) {
            self.feeds.published();
        }
        fn register_secret(&self, _provider: &str, _key: &str, _value: &str) {}
        fn is_managed_claude_account(&self, _id: &str, _project_id: Option<&str>) -> bool {
            false
        }
        fn add_claude_profile_account(
            &self,
            _project_id: Option<String>,
        ) -> Result<AccountInfo, String> {
            Err("not in this test".into())
        }
        fn remove_account(&self, _id: &str) {}
        fn record_claude_sign_in(
            &self,
            _id: &str,
            _email: Option<String>,
            _plan: Option<String>,
        ) -> Result<AccountInfo, String> {
            Err("not in this test".into())
        }
        fn record_codex_sign_in(
            &self,
            _auth_json: String,
            _project_id: Option<String>,
            _target: &SignInTarget,
        ) -> Result<AccountInfo, SignInRecordError> {
            Err(SignInRecordError::from("not in this test".to_string()))
        }
        fn sign_in_settled(
            &self,
            _sign_in_ref: &str,
            _provider: RoutedProvider,
            _outcome: SignInOutcome,
        ) {
        }
    }

    /// A host with two managed Claude accounts, both on generation 1.
    fn host() -> Host {
        let root = tempfile::tempdir().unwrap();
        let store = CredentialStoreHandle::new(root.path().to_path_buf(), None);
        let first = ClaudeOAuth::parse(
            &serde_json::json!({
                "accessToken": "at-1",
                "expiresAt": now_ms() + 8 * HOUR_MS,
            })
            .to_string(),
        )
        .unwrap()
        .stamped(1, now_ms());
        let mut accounts = CredentialStore {
            user_id: "feeds-test".to_string(),
            accounts: Vec::new(),
            git_identities: Vec::new(),
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: root.path().to_path_buf(),
        };
        for id in [ACCOUNT, OTHER] {
            let added = accounts.add_account(
                ApiProvider::Anthropic,
                id.to_string(),
                ProviderAuth::ClaudeProfile {
                    oauth: Some(first.to_json()),
                },
                None,
                0,
            );
            accounts
                .accounts
                .iter_mut()
                .find(|account| account.id == added.id)
                .unwrap()
                .id = id.to_string();
        }
        store.save(accounts).unwrap();
        Host {
            feeds: ClaudeCredentialFeeds::new(store.clone()),
            store,
            spawner: RealProcessSpawner,
            _root: root,
        }
    }

    /// A live session on `account_id`, launched on generation 1.
    fn live(host: &Host, session_id: &str, account_id: &str) -> (Pushes, FeedRegistration) {
        let pushes = Pushes::default();
        let control: Box<dyn SessionControl> = Box::new(RecordingControl(pushes.clone()));
        let registration = host.feeds.register(
            session_id,
            account_id,
            1,
            Arc::new(Mutex::new(Some(control))),
        );
        (pushes, registration)
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

    fn rotating(due: &[&str]) -> RotatingClaude {
        RotatingClaude {
            due: due.iter().map(|id| id.to_string()).collect(),
        }
    }

    #[test]
    fn a_launch_triggered_renewal_pushes_to_an_already_live_session() {
        let host = host();
        let (pushes, registration) = live(&host, "run-live", ACCOUNT);

        // A second launch on the account renews below the launch floor.
        let launched = rotating(&[])
            .claude_session_credential(&host, ACCOUNT)
            .unwrap();
        assert_eq!(launched.generation, 2);

        wait_for("the push", || !pushed(&pushes).is_empty());
        assert_eq!(pushed(&pushes), vec![(2, "at-2".to_string())]);
        assert_eq!(registration.feed().pushed(), 2);
        assert_eq!(
            registration.feed().acknowledged(),
            1,
            "not held until the CLI says so"
        );

        host.feeds.acknowledged("run-live", 2);
        assert_eq!(registration.feed().acknowledged(), 2);
    }

    #[test]
    fn a_sweep_pushes_every_session_on_a_renewed_account_once_and_no_other() {
        let host = host();
        let subscriptions = rotating(&[ACCOUNT]);
        let (first, _first) = live(&host, "run-a", ACCOUNT);
        let (second, _second) = live(&host, "run-b", ACCOUNT);
        let (other, other_registration) = live(&host, "run-c", OTHER);

        host.feeds.sweep(&host, &subscriptions);
        // The publish also reconciles from its own thread; let it run out.
        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(pushed(&first), vec![(2, "at-2".to_string())]);
        assert_eq!(pushed(&second), vec![(2, "at-2".to_string())]);
        assert!(pushed(&other).is_empty());
        assert_eq!(other_registration.feed().pushed(), 1);

        // The next renewal is pushed once too, and the one before is not
        // repeated.
        host.feeds.sweep(&host, &subscriptions);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            pushed(&first),
            vec![(2, "at-2".to_string()), (3, "at-3".to_string())]
        );
    }

    #[test]
    fn a_401_on_the_current_generation_renews_and_pushes_the_account() {
        let host = host();
        let (rejected, _rejected) = live(&host, "run-rejected", ACCOUNT);
        let (sibling, _sibling) = live(&host, "run-sibling", ACCOUNT);

        host.feeds.rejected(&host, &rotating(&[]), "run-rejected");
        wait_for("the sibling push", || !pushed(&sibling).is_empty());

        assert_eq!(pushed(&rejected), vec![(2, "at-2".to_string())]);
        assert_eq!(pushed(&sibling), vec![(2, "at-2".to_string())]);
    }

    #[test]
    fn a_401_on_a_superseded_generation_is_answered_without_renewing() {
        let host = host();
        let (pushes, registration) = live(&host, "run-behind", ACCOUNT);
        // Rotated elsewhere (another process sharing the store) without a
        // push reaching this one: no publish happened here.
        host.store
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

        host.feeds.rejected(&host, &rotating(&[]), "run-behind");

        assert_eq!(pushed(&pushes), vec![(2, "at-2".to_string())]);
        assert_eq!(registration.feed().pushed(), 2);
        let store = host.store.durable().unwrap();
        let account = store.accounts.iter().find(|a| a.id == ACCOUNT).unwrap();
        assert_eq!(
            stored_claude_oauth(account).unwrap().unwrap().generation(),
            2,
            "nothing was spent"
        );
    }

    #[test]
    fn a_dropped_registration_is_no_longer_fed() {
        let host = host();
        let (pushes, registration) = live(&host, "run-gone", ACCOUNT);
        drop(registration);

        host.feeds.sweep(&host, &rotating(&[ACCOUNT]));
        std::thread::sleep(Duration::from_millis(200));

        assert!(pushed(&pushes).is_empty());
    }

    #[test]
    fn dropping_a_displaced_registration_keeps_its_successor() {
        let host = host();
        let (_, displaced) = live(&host, "run-again", ACCOUNT);
        let (pushes, _successor) = live(&host, "run-again", ACCOUNT);
        drop(displaced);

        host.feeds.sweep(&host, &rotating(&[ACCOUNT]));
        wait_for("the push", || !pushed(&pushes).is_empty());

        assert_eq!(pushed(&pushes), vec![(2, "at-2".to_string())]);
    }

    #[test]
    fn an_observed_acknowledgement_is_recorded() {
        let host = host();
        let (_, registration) = live(&host, "run-ack", ACCOUNT);

        host.feeds.observe(
            &host,
            &rotating(&[]),
            "run-ack",
            &SessionEvent::CredentialAcknowledged { generation: 4 },
        );

        assert_eq!(registration.feed().acknowledged(), 4);
        assert_eq!(registration.feed().pushed(), 4);
    }
}
