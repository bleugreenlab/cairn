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
    fn server_closed(&self, server_id: &str) {
        self.orch
            .run_bearers
            .revoke_pool(server_id, "closed (pooled Codex server exited)");
    }
}

/// A live pooled server for a test to attribute calls to.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    struct QuietPoolHost;
    impl PoolHost for QuietPoolHost {
        fn refresh_tokens(&self, _client: &AppServerClient, _id: &Value) {}
        fn disconnected(&self, _runs: &[String]) {}
        fn standing_token_refusal(&self) -> Option<String> {
            None
        }
        fn server_closed(&self, _server_id: &str) {}
    }

    /// A server on `pools` that never reads its stdin: every write to it
    /// blocks until `gate`'s sender is dropped.
    pub(crate) fn unread_server(
        pools: &Arc<CodexAppServerPool>,
        gate: crossbeam_channel::Receiver<()>,
    ) -> Arc<PooledAppServer> {
        let client = AppServerClient::for_test_unread(gate);
        pools
            .ensure(&"identity-unread".to_string(), |_| {
                Ok((
                    Arc::new(client),
                    Arc::new(QuietPoolHost) as Arc<dyn PoolHost>,
                ))
            })
            .unwrap()
    }

    /// A server on `pools`. Its notification stream stays open, and the server
    /// with it, while the returned sender lives.
    pub(crate) fn live_server(
        pools: &Arc<CodexAppServerPool>,
    ) -> (Arc<PooledAppServer>, crossbeam_channel::Sender<Value>) {
        let (server, stream, _requests) = recording_server(pools);
        (server, stream)
    }

    /// [`live_server`], with every request sent to it recorded. It answers
    /// `thread/unsubscribe` and `thread/goal/get` (no goal) as Codex does and
    /// every other request with `{}`.
    pub(crate) fn recording_server(
        pools: &Arc<CodexAppServerPool>,
    ) -> (
        Arc<PooledAppServer>,
        crossbeam_channel::Sender<Value>,
        Arc<Mutex<Vec<Value>>>,
    ) {
        let requests: Arc<Mutex<Vec<Value>>> = Default::default();
        let (client, stream) = AppServerClient::for_test_with_notifications(
            |request| match request["method"].as_str() {
                Some("thread/unsubscribe") => {
                    Ok(serde_json::json!({ "result": { "status": "unsubscribed" } }))
                }
                Some("thread/goal/get") => Ok(serde_json::json!({ "result": { "goal": null } })),
                _ => Ok(serde_json::json!({ "result": {} })),
            },
            requests.clone(),
        );
        let server = pools
            .ensure(&"identity".to_string(), |_| {
                Ok((
                    Arc::new(client),
                    Arc::new(QuietPoolHost) as Arc<dyn PoolHost>,
                ))
            })
            .unwrap();
        (server, stream, requests)
    }
}
