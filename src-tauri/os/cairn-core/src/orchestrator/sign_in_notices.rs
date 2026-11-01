//! The operator's heads-up that a subscription sign-in is about to lapse.
//!
//! A Claude sign-in carries a deadline the provider declares, and renewing its
//! refresh token does not move it (see [`crate::credentials::sign_in`]). So the
//! only way to keep an account in rotation past it is a reconnect before it,
//! and the operator has to hear about that in time to act. Each account inside
//! its warning window is an entry in the notification center, derived on every
//! rebuild from the stored credential, so a reconnect that moves the deadline
//! resolves it without anything else being cleared.
//!
//! The entry key names the stage, so the operator is told once when the
//! account enters the three-day window, again inside the last day, and again
//! if it lapses or the provider refuses it: dismissing one stage does not
//! silence the next.

use std::sync::Mutex;

use crate::credentials::sign_in::{utc, SIGN_IN_FINAL_WARNING_SECS, SIGN_IN_WARNING_SECS};
use crate::credentials::{
    AccountHoldKind, CredentialStore, CredentialStoreHandle, ProviderAccount, SignInExpirySource,
    SignInStage,
};
use crate::notifications::{Hold, HoldSource};

/// The source's name in entry keys and override routing.
pub(crate) const SOURCE: &str = "sign_in_expiry";

/// The notification-center source of sign-in deadline notices.
pub(crate) struct SignInExpiryNotices {
    store: CredentialStoreHandle,
    /// The notice ids last announced, to tell the sweep when the center's
    /// answer changed with no write to announce it (a clock crossing a stage).
    announced: Mutex<Vec<String>>,
}

impl SignInExpiryNotices {
    pub(crate) fn new(store: CredentialStoreHandle) -> Self {
        Self {
            store,
            announced: Mutex::new(Vec::new()),
        }
    }

    /// Whether the notices at `now` differ from those last announced. The
    /// caller re-announces the center when they do.
    pub(crate) fn changed(&self, now: i64) -> bool {
        let current: Vec<String> = self
            .store
            .get()
            .map(|store| notices(&store, now))
            .unwrap_or_default()
            .into_iter()
            .map(|notice| notice.id)
            .collect();
        let Ok(mut announced) = self.announced.lock() else {
            return true;
        };
        if *announced == current {
            return false;
        }
        *announced = current;
        true
    }
}

impl HoldSource for SignInExpiryNotices {
    fn name(&self) -> &'static str {
        SOURCE
    }

    fn holds(&self, _project_id: &str) -> Vec<Hold> {
        self.store
            .get()
            .map(|store| notices(&store, chrono::Utc::now().timestamp()))
            .unwrap_or_default()
    }

    fn override_hold(&self, _hold_id: &str) -> Result<(), String> {
        Err("A sign-in deadline is the provider's and cannot be overridden. Reconnect the account from Settings → Providers.".to_string())
    }
}

fn provider_name(account: &ProviderAccount) -> &'static str {
    match account.api_provider {
        crate::credentials::ApiProvider::Anthropic => "Claude",
        crate::credentials::ApiProvider::OpenAI => "ChatGPT",
        _ => "subscription",
    }
}

/// `in 2 days`, `in 5 hours`, `in under an hour`.
pub(crate) fn time_left(now: i64, at: i64) -> String {
    let left = at - now;
    let days = left / (24 * 60 * 60);
    let hours = left / (60 * 60);
    match (days, hours) {
        (2.., _) => format!("in {days} days"),
        (1, _) => "in 1 day".to_string(),
        (_, 2..) => format!("in {hours} hours"),
        (_, 1) => "in 1 hour".to_string(),
        _ => "in under an hour".to_string(),
    }
}

/// Every sign-in notice `store` warrants at `now`: workspace-wide, one per
/// account inside its warning window, lapsed, or refused by its provider.
pub(crate) fn notices(store: &CredentialStore, now: i64) -> Vec<Hold> {
    store
        .accounts
        .iter()
        .filter_map(|account| notice(account, now))
        .collect()
}

fn notice(account: &ProviderAccount, now: i64) -> Option<Hold> {
    let who = account.email.as_deref().unwrap_or(&account.label);
    let provider = provider_name(account);
    let source_uri = Some(format!("cairn://models/accounts/{}", account.id));
    let hold = |id: String, reason: String, opened_at: i64| Hold {
        id,
        project_id: None,
        reason,
        expected_release: None,
        // Nothing to override: the act that clears it is a reconnect.
        override_label: String::new(),
        source_uri: source_uri.clone(),
        opened_at,
        released_at: None,
    };
    if let Some(refused) = account
        .health
        .as_ref()
        .and_then(|health| health.held.as_ref())
        .filter(|held| held.kind == AccountHoldKind::SignInExpired)
    {
        return Some(hold(
            format!("{}:refused:{}", account.id, refused.since),
            format!(
                "Reconnect {who} ({provider}) from Settings → Providers: the provider will no longer renew its sign-in ({}). Nothing is routed to it until then.",
                refused.detail
            ),
            refused.since,
        ));
    }
    let expiry = account.sign_in_expiry()?;
    let inferred = if expiry.source == SignInExpirySource::Inferred {
        " (inferred: the sign-in declared no deadline, so Cairn assumes 30 days)"
    } else {
        ""
    };
    let deadline = utc(expiry.expires_at);
    let (stage, opened_at, reason) = match expiry.stage(now) {
        SignInStage::Current => return None,
        SignInStage::Expiring => (
            "3d",
            expiry.expires_at - SIGN_IN_WARNING_SECS,
            format!(
                "Reconnect {who} ({provider}) by {deadline}: its sign-in expires {}{inferred}.",
                time_left(now, expiry.expires_at)
            ),
        ),
        SignInStage::ExpiringSoon => (
            "1d",
            expiry.expires_at - SIGN_IN_FINAL_WARNING_SECS,
            format!(
                "Reconnect {who} ({provider}) now: its sign-in expires {}, at {deadline}{inferred}.",
                time_left(now, expiry.expires_at)
            ),
        ),
        SignInStage::Lapsed => (
            "lapsed",
            expiry.expires_at,
            format!(
                "Reconnect {who} ({provider}) from Settings → Providers: its sign-in expired at {deadline}{inferred}. Nothing is routed to it until then."
            ),
        ),
    };
    Some(hold(
        format!("{}:{}:{stage}", account.id, expiry.expires_at),
        reason,
        opened_at,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::sign_in::CLAUDE_REFRESH_EXPIRES_AT_FIELD;
    use crate::credentials::{AccountHold, AccountSource, ApiProvider, ProviderAuth};

    const DAY: i64 = 24 * 60 * 60;

    fn account(expires_at: Option<i64>) -> ProviderAccount {
        let mut oauth = serde_json::json!({"accessToken": "at", "refreshToken": "rt"});
        if let Some(at) = expires_at {
            oauth[CLAUDE_REFRESH_EXPIRES_AT_FIELD] = serde_json::json!(at * 1000);
        }
        ProviderAccount {
            id: "acc_d2db".into(),
            label: "Claude".into(),
            api_provider: ApiProvider::Anthropic,
            source: AccountSource::Configured,
            auth: ProviderAuth::ClaudeProfile {
                oauth: Some(oauth.to_string()),
            },
            project_id: None,
            sort_order: 0,
            created_at: 0,
            last_used_at: None,
            email: Some("bleugreenagain@gmail.com".into()),
            plan: None,
            plan_choice: None,
            health: None,
        }
    }

    #[test]
    fn the_operator_hears_at_three_days_again_inside_the_last_day_and_at_the_lapse() {
        let deadline = 100 * DAY;
        let account = account(Some(deadline));
        assert!(notice(&account, deadline - 4 * DAY).is_none());

        let first = notice(&account, deadline - 2 * DAY).unwrap();
        assert_eq!(first.opened_at, deadline - 3 * DAY);
        assert!(
            first
                .reason
                .contains("Reconnect bleugreenagain@gmail.com (Claude) by"),
            "{}",
            first.reason
        );
        assert!(first.reason.contains("in 2 days"), "{}", first.reason);
        assert!(first.project_id.is_none(), "workspace-wide");
        assert!(first.override_label.is_empty());

        let second = notice(&account, deadline - 5 * 60 * 60).unwrap();
        assert_ne!(
            second.id, first.id,
            "a dismissed first notice does not silence the second"
        );
        assert!(second.reason.contains("in 5 hours"), "{}", second.reason);

        let lapsed = notice(&account, deadline).unwrap();
        assert_ne!(lapsed.id, second.id);
        assert!(lapsed.reason.contains("expired at"), "{}", lapsed.reason);
        assert!(
            lapsed.reason.contains("Nothing is routed to it"),
            "{}",
            lapsed.reason
        );
    }

    #[test]
    fn a_refused_sign_in_is_announced_whatever_its_deadline_says() {
        let mut refused = account(Some(1_000 * DAY));
        refused.health = Some(crate::credentials::ProviderAccountHealth {
            held: Some(AccountHold {
                since: 50,
                kind: AccountHoldKind::SignInExpired,
                detail: "Refresh token expired".into(),
            }),
            ..Default::default()
        });
        let notice = notice(&refused, 60).unwrap();
        assert!(
            notice.reason.contains("Refresh token expired"),
            "{}",
            notice.reason
        );
        assert_eq!(notice.opened_at, 50);
        // No deadline at all, and nothing refused: nothing to say.
        assert!(super::notice(&account(None), 60).is_none());
    }

    #[test]
    fn a_change_is_reported_once() {
        let root = tempfile::tempdir().unwrap();
        let mut store = CredentialStore {
            user_id: "u".into(),
            accounts: vec![account(Some(10 * DAY))],
            git_identities: Vec::new(),
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: root.path().to_path_buf(),
        };
        store.config_dir = root.path().to_path_buf();
        let notices = SignInExpiryNotices::new(CredentialStoreHandle::new(
            root.path().to_path_buf(),
            Some(store),
        ));
        assert!(!notices.changed(DAY), "nothing to announce yet");
        assert!(notices.changed(8 * DAY), "entered the window");
        assert!(!notices.changed(8 * DAY + 60));
        assert!(notices.changed(9 * DAY + 60), "inside the last day");
    }
}
