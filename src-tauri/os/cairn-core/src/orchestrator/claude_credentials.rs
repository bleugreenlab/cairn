//! The runtime's calls into [`cairn_models::claude::feeds`], which keeps live
//! Claude processes on their account's current credential. Everything that
//! decides what to push, and when, is the facet's; this file only hands it
//! the orchestrator as its `ModelHost` and the installed `Subscriptions`.
//!
//! A process joins the coordinator at launch (`backends/claude.rs` stores the
//! registration on its `RunHandle`) and leaves it when the handle leaves the
//! process registry.

use cairn_models::claude::feeds::ClaudeCredentialFeeds;

use super::Orchestrator;

impl Orchestrator {
    /// The coordinator every live Claude process on a managed account is
    /// registered with.
    pub(crate) fn claude_feeds(&self) -> &ClaudeCredentialFeeds {
        &self.claude_feeds
    }

    /// A live process was refused with a 401. Blocking.
    pub(crate) fn claude_credential_rejected(&self, run_id: &str) {
        self.claude_feeds
            .rejected(self, self.subscriptions(), run_id);
    }

    /// A live process confirmed it now holds `generation`.
    pub(crate) fn claude_credential_acknowledged(&self, run_id: &str, generation: u64) {
        self.claude_feeds.acknowledged(run_id, generation);
    }

    /// Renew what is close to expiry and bring every live process up to date.
    /// Blocking.
    pub(crate) fn sweep_claude_credentials(&self) {
        self.claude_feeds.sweep(self, self.subscriptions());
    }

    /// A credential was published in this process. Never blocks.
    pub(crate) fn claude_credentials_published(&self) {
        self.claude_feeds.published();
    }

    /// Once, at startup, before sessions launch.
    pub(crate) fn migrate_claude_homes(&self) {
        self.claude_feeds.migrate_homes(self, self.subscriptions());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use cairn_models::accounts::{ApiProvider, ProviderAuth};
    use cairn_models::claude::tokens::ClaudeOAuth;
    use cairn_models::interface::session::{SecretEnv, SessionControl, SessionInput};
    use cairn_models::subscription::testing::RotatingClaude;
    use cairn_models::subscription::ModelHost;
    use cairn_models::wire::MessageContent;
    use cairn_session::agent_process::process::{ControlHandle, RunHandle};

    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};

    const RUN: &str = "run-live";
    const HOUR_MS: i64 = 60 * 60 * 1000;

    /// Every generation pushed into one fake process.
    type Pushes = Arc<Mutex<Vec<u64>>>;

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
        fn update_credential(&mut self, _env: &SecretEnv, generation: u64) -> Result<(), String> {
            self.0.lock().unwrap().push(generation);
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    /// An orchestrator renewing through [`RotatingClaude`], with one managed
    /// Claude account on generation 1.
    async fn orchestrator() -> (Orchestrator, String) {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(db_state, services, config_dir)
            .subscriptions(Arc::new(RotatingClaude::default()))
            .build();
        let first = ClaudeOAuth::parse(
            &serde_json::json!({
                "accessToken": "at-1",
                "expiresAt": now_ms() + 8 * HOUR_MS,
            })
            .to_string(),
        )
        .unwrap()
        .stamped(1, now_ms());
        let account = orch
            .add_account(
                ApiProvider::Anthropic,
                "claude".to_string(),
                ProviderAuth::ClaudeProfile {
                    oauth: Some(first.to_json()),
                },
                None,
            )
            .unwrap()
            .id;
        (orch, account)
    }

    fn pushed(pushes: &Pushes) -> Vec<u64> {
        pushes.lock().unwrap().clone()
    }

    /// The wiring, not the rule (the facet's tests cover that): a process
    /// registered through its `RunHandle` is pushed when the orchestrator
    /// publishes, and stops being fed once it leaves the process registry.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_process_is_fed_while_its_handle_is_registered() {
        let (orch, account) = orchestrator().await;
        let pushes = Pushes::default();
        let control: ControlHandle = Arc::new(Mutex::new(Some(Box::new(RecordingControl(
            pushes.clone(),
        ))
            as Box<dyn SessionControl>)));
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            control.clone(),
            Some(format!("session-{RUN}")),
            None,
        );
        handle.credential_feed = Some(orch.claude_feeds().register(RUN, &account, 1, control));
        orch.process_state
            .register_process(RUN.into(), handle)
            .unwrap();

        // Publishes through the orchestrator's `ModelHost::credentials_published`.
        RotatingClaude::rotate(&orch, &account);
        let deadline = Instant::now() + Duration::from_secs(5);
        while pushed(&pushes).is_empty() {
            assert!(Instant::now() < deadline, "timed out waiting for the push");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(pushed(&pushes), vec![2]);

        assert!(orch.process_state.stop_and_remove(RUN));
        RotatingClaude::rotate(&orch, &account);
        orch.sweep_claude_credentials();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(pushed(&pushes), vec![2], "a removed process is not fed");
        assert_eq!(ModelHost::store(&orch).durable().unwrap().accounts.len(), 1);
    }
}
