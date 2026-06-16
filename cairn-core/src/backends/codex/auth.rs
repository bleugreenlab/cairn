use super::json_string;
use crate::identity::{ApiProvider, ProviderAuth};
use crate::orchestrator::Orchestrator;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

const CODEX_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
static CODEX_OAUTH_REFRESH_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone)]
pub struct CodexOAuthTokens {
    pub id_token: String,
    pub access_token: String,
    refresh_token: Option<String>,
    pub chatgpt_account_id: Option<String>,
}

pub(super) struct CodexAuthState {
    account_id: Option<String>,
    raw_json: String,
    tokens: CodexOAuthTokens,
}

impl CodexAuthState {
    pub(super) fn new_for_account(
        raw_json: &str,
        account_id: Option<String>,
    ) -> Result<Self, String> {
        let tokens = parse_codex_oauth_tokens(raw_json)?;
        Ok(Self {
            account_id,
            raw_json: raw_json.to_string(),
            tokens,
        })
    }

    pub(super) fn account_id(&self) -> Option<String> {
        self.account_id.clone()
    }

    pub(super) fn raw_json(&self) -> String {
        self.raw_json.clone()
    }

    pub(super) fn tokens(&self) -> CodexOAuthTokens {
        self.tokens.clone()
    }

    pub(super) fn id_access_pair(&self) -> (String, String) {
        (
            self.tokens.id_token.clone(),
            self.tokens.access_token.clone(),
        )
    }

    pub(super) fn chatgpt_account_id(&self) -> Option<String> {
        self.tokens.chatgpt_account_id.clone()
    }

    pub(super) fn refresh_token(&self) -> Option<String> {
        self.tokens.refresh_token.clone()
    }

    pub(super) fn apply_refresh(&mut self, new_tokens: CodexOAuthTokens) -> Result<String, String> {
        let mut value: Value = serde_json::from_str(&self.raw_json)
            .map_err(|e| format!("Failed to parse Codex auth JSON: {}", e))?;
        let tokens_obj = value
            .get_mut("tokens")
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| "Codex auth JSON missing tokens object".to_string())?;

        tokens_obj.insert(
            "id_token".into(),
            Value::String(new_tokens.id_token.clone()),
        );
        tokens_obj.insert(
            "access_token".into(),
            Value::String(new_tokens.access_token.clone()),
        );

        let refresh_to_store = new_tokens
            .refresh_token
            .clone()
            .or_else(|| self.tokens.refresh_token.clone());
        if let Some(refresh) = refresh_to_store.clone() {
            tokens_obj.insert("refresh_token".into(), Value::String(refresh));
        }

        let account_id = new_tokens
            .chatgpt_account_id
            .clone()
            .or_else(|| json_string(tokens_obj.get("account_id")))
            .or_else(|| self.tokens.chatgpt_account_id.clone());
        if let Some(account_id) = account_id.as_ref() {
            tokens_obj.insert("account_id".into(), Value::String(account_id.clone()));
        }
        value["last_refresh"] = Value::String(chrono::Utc::now().to_rfc3339());
        self.tokens = CodexOAuthTokens {
            id_token: new_tokens.id_token,
            access_token: new_tokens.access_token,
            refresh_token: refresh_to_store,
            chatgpt_account_id: account_id,
        };

        self.raw_json = serde_json::to_string(&value)
            .map_err(|e| format!("Failed to serialize Codex auth JSON: {}", e))?;
        Ok(self.raw_json.clone())
    }
}

pub(super) fn refresh_codex_tokens_via_http(
    refresh_token: &str,
) -> Result<CodexOAuthTokens, String> {
    let client = Client::new();
    let response = client
        .post(CODEX_OAUTH_TOKEN_URL)
        .json(&json!({
            "grant_type": "refresh_token",
            "client_id": CODEX_CLIENT_ID,
            "refresh_token": refresh_token,
        }))
        .send()
        .map_err(|e| format!("Refresh request failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_else(|_| "<no body>".to_string());
        return Err(format!(
            "Refresh token endpoint returned {}: {}",
            status, body
        ));
    }

    let value: Value = response
        .json()
        .map_err(|e| format!("Invalid refresh response: {}", e))?;

    let id_token = json_string(value.get("id_token"))
        .ok_or_else(|| "Refresh response missing id_token".to_string())?;
    let access_token = json_string(value.get("access_token"))
        .ok_or_else(|| "Refresh response missing access_token".to_string())?;
    let refresh_token = json_string(value.get("refresh_token"));

    let chatgpt_account_id = extract_chatgpt_account_id_from_jwt(&access_token);
    Ok(CodexOAuthTokens {
        id_token,
        access_token,
        refresh_token,
        chatgpt_account_id,
    })
}

pub(super) fn refresh_codex_tokens_for_session(
    orch: &Orchestrator,
    state_arc: &Arc<Mutex<CodexAuthState>>,
) -> Result<CodexOAuthTokens, String> {
    refresh_codex_tokens_for_session_with(orch, state_arc, refresh_codex_tokens_via_http)
}

pub fn refresh_codex_oauth_tokens_for_current_account(
    orch: &Orchestrator,
) -> Result<CodexOAuthTokens, String> {
    let auth_json = codex_oauth_json_from_store(orch, None)
        .ok_or_else(|| "Codex OAuth account not configured".to_string())?;
    let account_id = find_codex_oauth_account_id(orch, &auth_json);
    let state = Arc::new(Mutex::new(CodexAuthState::new_for_account(
        &auth_json, account_id,
    )?));
    refresh_codex_tokens_for_session(orch, &state)
}

pub(super) fn refresh_codex_tokens_for_session_with<F>(
    orch: &Orchestrator,
    state_arc: &Arc<Mutex<CodexAuthState>>,
    refresh_fn: F,
) -> Result<CodexOAuthTokens, String>
where
    F: FnOnce(&str) -> Result<CodexOAuthTokens, String>,
{
    let requested_refresh_token = state_arc
        .lock()
        .map_err(|_| "Codex auth state lock poisoned".to_string())?
        .refresh_token();

    let _refresh_guard = CODEX_OAUTH_REFRESH_LOCK
        .lock()
        .map_err(|_| "Codex OAuth refresh lock poisoned".to_string())?;

    let loaded_newer_tokens = sync_codex_auth_state_from_store(orch, state_arc)?;
    let current_tokens = state_arc
        .lock()
        .map_err(|_| "Codex auth state lock poisoned".to_string())?
        .tokens();

    if loaded_newer_tokens && current_tokens.refresh_token != requested_refresh_token {
        return Ok(current_tokens);
    }

    let refresh_token = current_tokens
        .refresh_token
        .clone()
        .ok_or_else(|| "Codex refresh token unavailable".to_string())?;
    let previous_json = state_arc
        .lock()
        .map_err(|_| "Codex auth state lock poisoned".to_string())?
        .raw_json();

    match refresh_fn(&refresh_token) {
        Ok(new_tokens) => {
            apply_and_persist_codex_refresh(orch, state_arc, new_tokens.clone(), &previous_json)?;
            let refreshed = state_arc
                .lock()
                .map_err(|_| "Codex auth state lock poisoned".to_string())?
                .tokens();
            Ok(refreshed)
        }
        Err(err) if is_refresh_token_reused_error(&err) => {
            if sync_codex_auth_state_from_store(orch, state_arc)? {
                let recovered_tokens = state_arc
                    .lock()
                    .map_err(|_| "Codex auth state lock poisoned".to_string())?
                    .tokens();
                if recovered_tokens.refresh_token.as_deref() != Some(refresh_token.as_str()) {
                    return Ok(recovered_tokens);
                }
            }
            Err(err)
        }
        Err(err) => Err(err),
    }
}

pub(super) fn apply_and_persist_codex_refresh(
    orch: &Orchestrator,
    state_arc: &Arc<Mutex<CodexAuthState>>,
    new_tokens: CodexOAuthTokens,
    previous_json: &str,
) -> Result<(), String> {
    let (account_id, json_str) = {
        let mut guard = state_arc
            .lock()
            .map_err(|_| "Codex auth state lock poisoned".to_string())?;
        let account_id = guard.account_id();
        let json_str = guard.apply_refresh(new_tokens)?;
        (account_id, json_str)
    };

    match persist_codex_oauth_tokens(orch, account_id.as_deref(), previous_json, &json_str) {
        Ok(true) => {}
        Ok(false) => {
            log::warn!("Refreshed Codex tokens were not persisted: no OpenAI OAuth account found")
        }
        Err(err) => log::warn!("Failed to persist refreshed Codex tokens: {}", err),
    }
    Ok(())
}

pub(super) fn sync_codex_auth_state_from_store(
    orch: &Orchestrator,
    state_arc: &Arc<Mutex<CodexAuthState>>,
) -> Result<bool, String> {
    let (account_id, current_json) = {
        let guard = state_arc
            .lock()
            .map_err(|_| "Codex auth state lock poisoned".to_string())?;
        (guard.account_id(), guard.raw_json())
    };

    let Some(latest_json) = codex_oauth_json_from_store(orch, account_id.as_deref()) else {
        return Ok(false);
    };
    if latest_json == current_json {
        return Ok(false);
    }

    let latest_state = CodexAuthState::new_for_account(&latest_json, account_id)?;
    let mut guard = state_arc
        .lock()
        .map_err(|_| "Codex auth state lock poisoned".to_string())?;
    *guard = latest_state;
    Ok(true)
}

pub(super) fn is_refresh_token_reused_error(err: &str) -> bool {
    err.contains("refresh_token_reused")
}

pub(super) fn find_codex_oauth_account_id(orch: &Orchestrator, auth_json: &str) -> Option<String> {
    let store = orch.get_identity_store()?;
    store
        .accounts
        .iter()
        .find(|account| {
            account.api_provider == ApiProvider::OpenAI
                && matches!(
                    &account.auth,
                    ProviderAuth::OAuthToken { value } if value == auth_json
                )
        })
        .map(|account| account.id.clone())
}

pub(super) fn codex_oauth_json_from_store(
    orch: &Orchestrator,
    account_id: Option<&str>,
) -> Option<String> {
    let store = orch.get_identity_store()?;

    if let Some(account_id) = account_id {
        if let Some(value) = store.accounts.iter().find_map(|account| {
            if account.id == account_id && account.api_provider == ApiProvider::OpenAI {
                if let ProviderAuth::OAuthToken { value } = &account.auth {
                    return Some(value.clone());
                }
            }
            None
        }) {
            return Some(value);
        }
    }

    store
        .accounts
        .iter()
        .filter_map(|account| {
            if account.api_provider == ApiProvider::OpenAI {
                if let ProviderAuth::OAuthToken { value } = &account.auth {
                    return Some((account.sort_order, value.clone()));
                }
            }
            None
        })
        .min_by_key(|(sort_order, _)| *sort_order)
        .map(|(_, value)| value)
}

pub(super) fn persist_codex_oauth_tokens(
    orch: &Orchestrator,
    account_id: Option<&str>,
    previous_json: &str,
    new_json: &str,
) -> Result<bool, String> {
    let Some(mut store) = orch.get_identity_store() else {
        return Ok(false);
    };

    let target_idx = account_id
        .and_then(|id| {
            store.accounts.iter().position(|account| {
                account.id == id
                    && account.api_provider == ApiProvider::OpenAI
                    && matches!(&account.auth, ProviderAuth::OAuthToken { .. })
            })
        })
        .or_else(|| {
            store.accounts.iter().position(|account| {
                account.api_provider == ApiProvider::OpenAI
                    && matches!(
                        &account.auth,
                        ProviderAuth::OAuthToken { value } if value == previous_json
                    )
            })
        })
        .or_else(|| {
            store
                .accounts
                .iter()
                .enumerate()
                .filter(|(_, account)| {
                    account.api_provider == ApiProvider::OpenAI
                        && matches!(&account.auth, ProviderAuth::OAuthToken { .. })
                })
                .min_by_key(|(_, account)| account.sort_order)
                .map(|(idx, _)| idx)
        });

    let Some(target_idx) = target_idx else {
        return Ok(false);
    };

    store.accounts[target_idx].auth = ProviderAuth::OAuthToken {
        value: new_json.to_string(),
    };

    orch.save_identity_store(store)?;
    let _ = orch.services.emitter.emit(
        "config-changed",
        serde_json::json!({"entity_type": "identity"}),
    );
    Ok(true)
}

pub(super) fn parse_codex_oauth_tokens(auth_json: &str) -> Result<CodexOAuthTokens, String> {
    let value: Value =
        serde_json::from_str(auth_json).map_err(|e| format!("Invalid Codex OAuth JSON: {}", e))?;
    let tokens = value
        .get("tokens")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "Missing tokens field".to_string())?;
    let id_token =
        json_string(tokens.get("id_token")).ok_or_else(|| "Missing id_token".to_string())?;
    let access_token = json_string(tokens.get("access_token"))
        .ok_or_else(|| "Missing access_token".to_string())?;
    let refresh_token = json_string(tokens.get("refresh_token"));
    let chatgpt_account_id = token_chatgpt_account_id(tokens, &access_token);
    if chatgpt_account_id.is_none() {
        return Err("Missing ChatGPT account id in Codex auth tokens".to_string());
    }
    Ok(CodexOAuthTokens {
        id_token,
        access_token,
        refresh_token,
        chatgpt_account_id,
    })
}

/// Extract `chatgpt_account_id` from a Codex access token JWT without signature verification.
pub(super) fn extract_chatgpt_account_id_from_jwt(jwt: &str) -> Option<String> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let payload_b64 = jwt.split('.').nth(1)?;
    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let payload: Value = serde_json::from_slice(&payload_bytes).ok()?;
    json_string(payload.pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id"))
}

pub(super) fn token_chatgpt_account_id(
    tokens: &serde_json::Map<String, Value>,
    access_token: &str,
) -> Option<String> {
    extract_chatgpt_account_id_from_jwt(access_token)
        .or_else(|| json_string(tokens.get("account_id")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    fn unsigned_jwt(payload: &str) -> String {
        format!("header.{}.signature", URL_SAFE_NO_PAD.encode(payload))
    }

    #[test]
    fn parses_chatgpt_account_id_from_auth_json_tokens_account_id() {
        let access_token = unsigned_jwt(r#"{"sub":"user"}"#);
        let auth_json = serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": "id-token",
                "access_token": access_token,
                "refresh_token": "refresh-token",
                "account_id": "account-from-auth-json"
            }
        });

        let tokens = parse_codex_oauth_tokens(&auth_json.to_string()).unwrap();

        assert_eq!(
            tokens.chatgpt_account_id.as_deref(),
            Some("account-from-auth-json")
        );
    }
}
