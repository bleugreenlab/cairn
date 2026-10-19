//! Credential refresh and workflow failure policy for the session-owned pool.
use crate::orchestrator::Orchestrator;
use cairn_models::codex::app_server::AppServerClient;
use cairn_models::codex::token_requests::{standing_refusal_notice, token_request_reply};
use cairn_models::codex::tokens::CodexAuthState;
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
        let reply = token_request_reply(orch, orch.subscriptions(), self.oauth_state.as_ref());
        if let Err(e) = client.reply(id, reply) {
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
    fn standing_token_refusal(&self) -> Option<String> {
        standing_refusal_notice(self.oauth_state.as_ref())
    }
}
