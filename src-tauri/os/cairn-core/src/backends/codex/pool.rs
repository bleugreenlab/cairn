//! Credential refresh and workflow failure policy for the session-owned pool.
use super::refresh_codex_tokens_for_session;
use crate::orchestrator::Orchestrator;
use cairn_llm::codex::app_server::AppServerClient;
use cairn_llm::codex::tokens::CodexAuthState;
pub use cairn_session::backends::codex::pool::*;
use serde_json::Value;
use std::sync::{Arc, Mutex};

pub(super) fn pool_host(
    orch: &Orchestrator,
    oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
) -> Arc<dyn PoolHost> {
    Arc::new(CorePoolHost {
        orch: orch.clone(),
        oauth_state,
    })
}
struct CorePoolHost {
    orch: Orchestrator,
    oauth_state: Option<Arc<Mutex<CodexAuthState>>>,
}
impl PoolHost for CorePoolHost {
    fn refresh_tokens(&self, client: &AppServerClient, id: &Value) {
        let orch = &self.orch;
        let response = match self.oauth_state.as_ref() {
        Some(state_arc) => match refresh_codex_tokens_for_session(orch, state_arc) {
            Ok(new_tokens) => match new_tokens.chatgpt_account_id {
                Some(account_id) => client.respond(
                    id,
                    serde_json::json!({
                        "accessToken": new_tokens.access_token,
                        "chatgptAccountId": account_id,
                    }),
                ),
                None => client.respond_error(
                    id,
                    -32000,
                    "Codex token refresh did not provide a ChatGPT account id; reconnect that account from Settings → Providers",
                ),
            },
            // The authority has already named the account and said whether this
            // is worth an operator's attention; repeating a reconnect
            // instruction over it would undo that.
            Err(err) => client.respond_error(id, -32000, &err.operator_message()),
        },
        None => client.respond_error(
            id,
            -32000,
            "Codex OAuth tokens unavailable; connect a ChatGPT account from Settings → Providers",
        ),
    };
        if let Err(e) = response {
            log::warn!("codex pool: failed to answer token refresh: {}", e);
        }
    }
    fn disconnected(&self, runs: &[String]) {
        for run_id in runs {
            crate::orchestrator::lifecycle::fail_run(
                &self.orch,
                run_id,
                "codex pooled app-server closed",
            );
        }
    }
}
