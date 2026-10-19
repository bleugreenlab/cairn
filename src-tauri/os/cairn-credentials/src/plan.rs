//! Which subscription plan an account is on, read from the credential it
//! holds.
//!
//! The plan is the provider's own word, already stored: a managed Claude
//! credential carries `subscriptionType` and `rateLimitTier`, and a Codex
//! `auth.json` carries an id token whose `chatgpt_plan_type` claim names the
//! ChatGPT plan. Reading it from the stored credential means an account
//! signed in before Cairn priced plans resolves without signing in again, and
//! a plan the provider changes is seen at the next token rotation.

use cairn_models_types::subscription::{
    resolve, settle, AccountPlan, PlanResolution, SubscriptionProvider,
};
use serde_json::Value;

use crate::{AccountInfo, ApiProvider, CredentialStore, ProviderAccount, ProviderAuth};

/// Decode a JWT's claims without verifying its signature.
///
/// Only for tokens the provider issued to Cairn over TLS, read to label and
/// key the account they belong to; the provider verifies them on every call
/// that spends them.
pub fn jwt_claims(jwt: &str) -> Option<Value> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let payload = jwt.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// The id-token claim ChatGPT names the plan in.
const CHATGPT_PLAN_CLAIM: &str = "/https:~1~1api.openai.com~1auth/chatgpt_plan_type";

fn nonempty(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// What `account`'s subscription plan is, as its provider says, or `None`
/// when the account is not a subscription (an API key billed per token, a
/// local host) or is a Claude row whose sign-in has not completed.
pub fn subscription_plan(
    account: &ProviderAccount,
) -> Option<(SubscriptionProvider, PlanResolution)> {
    match (&account.api_provider, &account.auth) {
        (ApiProvider::Anthropic, ProviderAuth::ClaudeProfile { oauth }) => {
            let stored: Option<Value> = oauth
                .as_deref()
                .and_then(|json| serde_json::from_str(json).ok());
            let plan = stored
                .as_ref()
                .and_then(|stored| nonempty(stored.get("subscriptionType")))
                .or_else(|| account.plan.clone());
            let tier = stored
                .as_ref()
                .and_then(|stored| nonempty(stored.get("rateLimitTier")));
            if oauth.is_none() && plan.is_none() {
                return None;
            }
            Some((
                SubscriptionProvider::Claude,
                resolve(
                    SubscriptionProvider::Claude,
                    plan.as_deref(),
                    tier.as_deref(),
                ),
            ))
        }
        (ApiProvider::OpenAI, ProviderAuth::OAuthToken { value }) => {
            let claimed = serde_json::from_str::<Value>(value)
                .ok()
                .and_then(|auth| nonempty(auth.pointer("/tokens/id_token")))
                .and_then(|id_token| jwt_claims(&id_token))
                .and_then(|claims| nonempty(claims.pointer(CHATGPT_PLAN_CLAIM)));
            let plan = claimed.or_else(|| account.plan.clone());
            Some((
                SubscriptionProvider::Codex,
                resolve(SubscriptionProvider::Codex, plan.as_deref(), None),
            ))
        }
        (ApiProvider::OpenCode, ProviderAuth::ApiKey { .. }) => Some((
            SubscriptionProvider::OpenCodeGo,
            resolve(SubscriptionProvider::OpenCodeGo, None, None),
        )),
        _ => None,
    }
}

impl ProviderAccount {
    /// This account's plan with the operator's choice applied, or `None`
    /// when it is not a subscription.
    pub fn account_plan(&self) -> Option<AccountPlan> {
        let (provider, resolution) = subscription_plan(self)?;
        Some(settle(provider, &resolution, self.plan_choice.as_deref()))
    }

    /// Whether the stored choice is still one of this account's candidates.
    fn plan_choice_is_current(&self) -> bool {
        self.plan_choice.as_deref().is_none_or(|choice| {
            subscription_plan(self).is_some_and(|(_, resolution)| resolution.admits_choice(choice))
        })
    }
}

impl CredentialStore {
    /// Record which of its candidate plans an ambiguous account is on, or
    /// clear the choice with `None`. A key that is not one of the account's
    /// candidates is refused. The caller persists the store.
    pub fn set_plan_choice(
        &mut self,
        id: &str,
        choice: Option<String>,
    ) -> Result<AccountInfo, String> {
        let account = self
            .accounts
            .iter_mut()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        if let Some(choice) = choice.as_deref() {
            let Some((_, resolution)) = subscription_plan(account) else {
                return Err(format!("Account {id} is not a subscription"));
            };
            if !resolution.admits_choice(choice) {
                let candidates = match &resolution {
                    PlanResolution::Ambiguous { candidates, .. } => candidates.join(", "),
                    _ => String::new(),
                };
                return Err(if candidates.is_empty() {
                    format!("Account {id}'s plan is not a choice")
                } else {
                    format!("`{choice}` is not one of account {id}'s plans: {candidates}")
                });
            }
        }
        account.plan_choice = choice;
        Ok(AccountInfo::from(&*account))
    }

    /// Drop every stored plan choice that is no longer among its account's
    /// candidates, because the provider's plan changed. Answers whether any
    /// was dropped. The caller persists the store.
    pub fn clear_stale_plan_choices(&mut self) -> bool {
        let mut cleared = false;
        for account in &mut self.accounts {
            if !account.plan_choice_is_current() {
                account.plan_choice = None;
                cleared = true;
            }
        }
        cleared
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AccountSource;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    fn account(api_provider: ApiProvider, auth: ProviderAuth) -> ProviderAccount {
        ProviderAccount {
            id: "acc_1".to_string(),
            label: "one".to_string(),
            api_provider,
            source: AccountSource::Configured,
            auth,
            project_id: None,
            sort_order: 0,
            created_at: 0,
            last_used_at: None,
            email: None,
            plan: None,
            plan_choice: None,
            health: None,
        }
    }

    fn claude(oauth: Option<&str>) -> ProviderAccount {
        account(
            ApiProvider::Anthropic,
            ProviderAuth::ClaudeProfile {
                oauth: oauth.map(str::to_string),
            },
        )
    }

    fn codex(plan_type: &str) -> ProviderAccount {
        let claims =
            format!(r#"{{"https://api.openai.com/auth":{{"chatgpt_plan_type":"{plan_type}"}}}}"#);
        let id_token = format!("h.{}.s", URL_SAFE_NO_PAD.encode(claims));
        let auth = serde_json::json!({"tokens": {"id_token": id_token, "access_token": "a"}});
        account(
            ApiProvider::OpenAI,
            ProviderAuth::OAuthToken {
                value: auth.to_string(),
            },
        )
    }

    fn store(accounts: Vec<ProviderAccount>) -> CredentialStore {
        CredentialStore {
            user_id: "user".to_string(),
            accounts,
            git_identities: Vec::new(),
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir: std::path::PathBuf::new(),
        }
    }

    #[test]
    fn a_stored_claude_credential_names_its_plan() {
        let max = claude(Some(
            r#"{"accessToken":"at","subscriptionType":"max","rateLimitTier":"default_claude_max_20x"}"#,
        ));
        let plan = max.account_plan().expect("a subscription");
        assert_eq!(
            (plan.label.as_str(), plan.monthly_usd),
            ("Max 20x", Some(200))
        );
        assert_eq!(plan.provider, "claude");

        // Without a tier, Max is a choice between its two plans.
        let untiered = claude(Some(r#"{"accessToken":"at","subscriptionType":"max"}"#));
        assert_eq!(untiered.account_plan().unwrap().options.len(), 2);

        // The sign-in's recorded plan stands in for a credential without one.
        let mut recorded = claude(Some(r#"{"accessToken":"at"}"#));
        recorded.plan = Some("pro".to_string());
        assert_eq!(recorded.account_plan().unwrap().monthly_usd, Some(20));

        // A row whose sign-in never completed is not a subscription yet.
        assert_eq!(claude(None).account_plan(), None);
    }

    #[test]
    fn a_codex_id_token_names_its_plan() {
        let plus = codex("plus").account_plan().unwrap();
        assert_eq!((plus.label.as_str(), plus.monthly_usd), ("Plus", Some(20)));
        let lite = codex("prolite").account_plan().unwrap();
        assert_eq!(
            (lite.label.as_str(), lite.monthly_usd),
            ("Pro 100", Some(100))
        );
        let pro = codex("pro").account_plan().unwrap();
        assert_eq!(pro.label, "Pro");
        assert_eq!(pro.options.len(), 2);
        let enterprise = codex("ent26").account_plan().unwrap();
        assert_eq!(
            (enterprise.label.as_str(), enterprise.monthly_usd),
            ("Enterprise", None)
        );
    }

    #[test]
    fn metered_and_local_accounts_have_no_plan() {
        for (provider, auth) in [
            (
                ApiProvider::Anthropic,
                ProviderAuth::ApiKey { value: "k".into() },
            ),
            (
                ApiProvider::OpenAI,
                ProviderAuth::ApiKey { value: "k".into() },
            ),
            (
                ApiProvider::OpenRouter,
                ProviderAuth::ApiKey { value: "k".into() },
            ),
            (
                ApiProvider::Ollama,
                ProviderAuth::BaseUrl {
                    url: "http://h".into(),
                },
            ),
        ] {
            assert_eq!(account(provider, auth).account_plan(), None);
        }
        let go = account(
            ApiProvider::OpenCode,
            ProviderAuth::ApiKey { value: "k".into() },
        );
        assert_eq!(go.account_plan().unwrap().label, "Go");
    }

    #[test]
    fn a_plan_choice_must_be_a_candidate_and_is_dropped_when_the_plan_changes() {
        let mut store = store(vec![codex("pro")]);
        let refused = store.set_plan_choice("acc_1", Some("plus".to_string()));
        assert!(refused.unwrap_err().contains("pro_200, pro_500"));
        assert!(store.set_plan_choice("missing", None).is_err());

        let info = store
            .set_plan_choice("acc_1", Some("pro_500".to_string()))
            .unwrap();
        let shown = serde_json::to_value(&info).unwrap();
        assert_eq!(shown["subscription"]["label"], "Pro 500");
        assert_eq!(shown["subscription"]["monthlyUsd"], 500);
        assert!(!store.clear_stale_plan_choices());

        // The account moved to Plus: the Pro choice no longer applies.
        let mut moved = codex("plus");
        moved.plan_choice = Some("pro_500".to_string());
        store.accounts[0] = moved;
        assert_eq!(
            store.accounts[0].account_plan().unwrap().monthly_usd,
            Some(20)
        );
        assert!(store.clear_stale_plan_choices());
        assert_eq!(store.accounts[0].plan_choice, None);

        // Clearing a choice is always allowed.
        store.accounts[0] = codex("pro");
        store.accounts[0].plan_choice = Some("pro_200".to_string());
        store.set_plan_choice("acc_1", None).unwrap();
        assert_eq!(store.accounts[0].plan_choice, None);

        // An account that is not a subscription has no plan to choose.
        store.accounts[0] = account(
            ApiProvider::OpenRouter,
            ProviderAuth::ApiKey { value: "k".into() },
        );
        assert!(store
            .set_plan_choice("acc_1", Some("go".to_string()))
            .unwrap_err()
            .contains("not a subscription"));
    }
}
