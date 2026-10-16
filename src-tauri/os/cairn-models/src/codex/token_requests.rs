//! Answering a Codex app-server's request for fresh ChatGPT tokens.
//!
//! A Codex session authenticates from inside its app-server, on the login the
//! host handed it at launch. When those tokens stop working the app-server
//! asks the host for new ones with an `account/chatgptAuthTokens/refresh`
//! server request and holds the turn until it is answered. Renewing is
//! subscription mechanics, behind [`Subscriptions::refresh_codex`]: the one
//! spender of the account's rotating credential, which also coordinates
//! concurrent requests across the machine. This module is everything around
//! that call, for Cairn's runtime and for any host built on the facet alike:
//! what the renewal is asked for, what the session adopts, and what the
//! app-server is told.
//!
//! What a host owes a Codex session once it is launched:
//!
//! 1. Hold the session's [`CodexAuthState`] in an `Arc<Mutex<_>>` for as long
//!    as the session lives.
//! 2. Pass every [`SessionEvent`] the session emits to [`observe`], with the
//!    session's control. It answers the token request and leaves every other
//!    event alone.
//! 3. Never call it on an async executor's thread: a renewal can wait on the
//!    machine-wide lease another process holds.
//!
//! Cairn's own pooled and dedicated readers answer through
//! [`token_request_reply`] from the seams they route the request to.

use std::sync::{Arc, Mutex};

use serde_json::json;

use crate::codex::tokens::{refresh_session_tokens, CodexAuthState};
use crate::interface::session::{ServerReply, SessionControl};
use crate::session::events::SessionEvent;
use crate::subscription::{ModelHost, Subscriptions};
use crate::wire::codex_app_server::AUTH_TOKENS_REFRESH_METHOD;

/// The JSON-RPC error code a token request is refused with.
pub const TOKEN_REQUEST_REFUSED: i32 = -32000;

const NO_TOKENS_HELD: &str =
    "No ChatGPT tokens are held for this session; connect a ChatGPT account";
const NO_ACCOUNT_ID: &str =
    "Codex token renewal did not provide a ChatGPT account id; reconnect that ChatGPT account";

fn refused(message: impl Into<String>) -> ServerReply {
    ServerReply::Error {
        code: TOKEN_REQUEST_REFUSED,
        message: message.into(),
    }
}

/// The answer to an app-server's request for fresh ChatGPT tokens for the
/// session running on `auth`: renew through [`Subscriptions::refresh_codex`],
/// adopt what is now durable, and reply with it. A session with no tokens of
/// its own, or a renewal that failed, is refused with a message an operator
/// can act on; the request is never left unanswered.
///
/// Blocks while the renewal runs.
pub fn token_request_reply(
    host: &dyn ModelHost,
    subscriptions: &dyn Subscriptions,
    auth: Option<&Arc<Mutex<CodexAuthState>>>,
) -> ServerReply {
    let Some(auth) = auth else {
        return refused(NO_TOKENS_HELD);
    };
    match refresh_session_tokens(host, auth, |request| {
        subscriptions.refresh_codex(host, request)
    }) {
        Ok(tokens) => match tokens.chatgpt_account_id {
            Some(account_id) => ServerReply::Result(json!({
                "accessToken": tokens.access_token,
                "chatgptAccountId": account_id,
            })),
            None => refused(NO_ACCOUNT_ID),
        },
        // The authority has already named the account and graded the failure;
        // wrapping a reconnect instruction around it would undo that.
        Err(failure) => refused(failure.operator_message()),
    }
}

/// Answer `event` through `control` if it is the token request. Answers
/// whether it was; any other event is left alone.
pub fn observe(
    host: &dyn ModelHost,
    subscriptions: &dyn Subscriptions,
    auth: &Arc<Mutex<CodexAuthState>>,
    event: &SessionEvent,
    control: &mut dyn SessionControl,
) -> Result<bool, String> {
    let SessionEvent::ServerRequest { id, method, .. } = event else {
        return Ok(false);
    };
    if method != AUTH_TOKENS_REFRESH_METHOD {
        return Ok(false);
    }
    control.reply(id, token_request_reply(host, subscriptions, Some(auth)))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    use cairn_platform::services::process::{ProcessSpawner, RealProcessSpawner};
    use serde_json::Value;

    use crate::accounts::{AccountInfo, CredentialStoreHandle, RoutedProvider};
    use crate::interface::session::SessionInput;
    use crate::subscription::testing::RenewingCodex;
    use crate::subscription::{SignInOutcome, SignInRecordError, SignInTarget};
    use crate::wire::MessageContent;

    /// The least a host is: a store, and every secret it is handed.
    struct Host {
        store: CredentialStoreHandle,
        spawner: RealProcessSpawner,
        secrets: Mutex<Vec<String>>,
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
        fn credentials_published(&self) {}
        fn register_secret(&self, _provider: &str, _key: &str, value: &str) {
            self.secrets.lock().unwrap().push(value.to_string());
        }
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

    fn host() -> Host {
        let root = tempfile::tempdir().unwrap();
        Host {
            store: CredentialStoreHandle::new(root.path().to_path_buf(), None),
            spawner: RealProcessSpawner,
            secrets: Mutex::default(),
            _root: root,
        }
    }

    /// A session on ChatGPT account `chatgpt-1`, holding access token `at-1`.
    fn session_auth(host: &Host) -> Arc<Mutex<CodexAuthState>> {
        let login = json!({"tokens": {
            "id_token": "id-1",
            "access_token": "at-1",
            "account_id": "chatgpt-1",
        }});
        Arc::new(Mutex::new(
            CodexAuthState::new_for_account(host, &login.to_string(), Some("acct-cairn".into()))
                .unwrap(),
        ))
    }

    fn refusal(reply: &ServerReply) -> (i32, &str) {
        match reply {
            ServerReply::Error { code, message } => (*code, message),
            ServerReply::Result(_) => panic!("the request was answered with tokens"),
        }
    }

    #[test]
    fn a_token_request_is_answered_with_the_renewed_tokens() {
        let host = host();
        let auth = session_auth(&host);
        let authority = RenewingCodex::default();

        let reply = token_request_reply(&host, &authority, Some(&auth));

        assert!(
            reply
                == ServerReply::Result(
                    json!({"accessToken": "at-2", "chatgptAccountId": "chatgpt-1"})
                ),
            "the reply carries the renewed access token and the account"
        );
        let asked = authority.asked();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].cairn_account_id.as_deref(), Some("acct-cairn"));
        assert_eq!(asked[0].chatgpt_account_id.as_deref(), Some("chatgpt-1"));
        assert_eq!(auth.lock().unwrap().tokens().access_token, "at-2");
        assert!(host
            .secrets
            .lock()
            .unwrap()
            .iter()
            .any(|secret| secret == "at-2"));
    }

    #[test]
    fn a_session_without_tokens_is_refused() {
        let host = host();
        let authority = RenewingCodex::default();

        let reply = token_request_reply(&host, &authority, None);

        assert_eq!(refusal(&reply), (TOKEN_REQUEST_REFUSED, NO_TOKENS_HELD));
        assert!(authority.asked().is_empty());
    }

    #[test]
    fn a_failed_renewal_is_refused_with_the_authority_s_message() {
        let host = host();
        let auth = session_auth(&host);
        let authority = RenewingCodex::refusing();

        let reply = token_request_reply(&host, &authority, Some(&auth));

        let (code, message) = refusal(&reply);
        assert_eq!(code, TOKEN_REQUEST_REFUSED);
        assert!(message.contains("can no longer be renewed"), "{message}");
        assert!(message.contains("acct-cairn"), "{message}");
        assert!(!message.contains("at-1"), "{message}");
        assert_eq!(auth.lock().unwrap().tokens().access_token, "at-1");
    }

    /// Every `(id, reply)` sent to the session.
    #[derive(Default)]
    struct RecordingControl(Vec<(Value, ServerReply)>);

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
        fn reply(&mut self, id: &Value, reply: ServerReply) -> Result<(), String> {
            self.0.push((id.clone(), reply));
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    #[test]
    fn observe_answers_the_token_request_and_nothing_else() {
        let host = host();
        let auth = session_auth(&host);
        let authority = RenewingCodex::default();
        let mut control = RecordingControl::default();
        let approval = SessionEvent::ServerRequest {
            id: json!(7),
            method: "item/commandExecution/requestApproval".to_string(),
            params: json!({}),
        };
        let token_request = SessionEvent::ServerRequest {
            id: json!(8),
            method: AUTH_TOKENS_REFRESH_METHOD.to_string(),
            params: json!({"reason": "unauthorized"}),
        };

        assert!(!observe(&host, &authority, &auth, &approval, &mut control).unwrap());
        assert!(control.0.is_empty());
        assert!(observe(&host, &authority, &auth, &token_request, &mut control).unwrap());
        assert_eq!(control.0.len(), 1);
        assert_eq!(control.0[0].0, json!(8));
        assert!(matches!(control.0[0].1, ServerReply::Result(_)));
    }
}
