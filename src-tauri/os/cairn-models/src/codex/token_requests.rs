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
//! 2. Answer the token request with [`token_request_reply`] from the scoped
//!    handler of the [`ThreadRouter`](crate::codex::thread_router::ThreadRouter)
//!    the session's app-server is read through, shared or the session's own.
//!    The handler sees the request as it arrives, even while `thread/start`
//!    is still pending, and Codex gives up on a refresh after ten seconds.
//! 3. Never answer on an async executor's thread: a renewal can wait on the
//!    machine-wide lease another process holds. A router's dispatcher is a
//!    thread of its own.
//!
//! That handler is the one place the request is answered. Every Codex
//! app-server is read through a router (a dedicated session's through
//! [`ThreadRouter::start_owned`](crate::codex::thread_router::ThreadRouter::start_owned)),
//! and a handler that passes the request on leaves it to the session's
//! reader, which starts only once the thread is open, too late for a refresh
//! asked for during the start. Cairn's pooled and dedicated servers answer
//! this way, and so does the external example host (`examples/llm-host`).
//!
//! Before any of that, a host launching a Codex app-server on a subscription
//! passes the session's tokens through [`renew_for_launch`], so the app-server
//! is never signed in on an access token that has already expired.
//!
//! The app-server drops a refusal's message and reports its own error for the
//! turn instead (a routing or auth failure that names nothing Cairn did). So
//! every refusal is logged here with which branch refused and for which
//! account, every renewal is logged with the generation it adopted, and the
//! refusal is remembered on the session's [`CodexAuthState`] until a renewal
//! succeeds. A host whose session's turn fails meanwhile reports
//! [`standing_refusal_notice`] beside the app-server's error.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;

use crate::codex::tokens::{
    codex_refresh_generation, refresh_session_tokens, CodexAuthState, CodexOAuthTokens,
    CodexRefreshFailure, CodexRefreshFailureKind,
};
use crate::interface::session::ServerReply;
use crate::subscription::{ModelHost, Subscriptions};

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

/// Refuse a token request, saying so in the log and on the session.
///
/// `branch` names which refusal fired, so the log alone answers "why did
/// Cairn refuse". Only the operator message is logged: it names the account
/// and never a token.
fn refuse(
    auth: Option<&Arc<Mutex<CodexAuthState>>>,
    branch: &str,
    account: Option<&str>,
    message: String,
) -> ServerReply {
    log::warn!(
        "Refused a Codex token request ({branch}) for Cairn account {}: {message}",
        account.unwrap_or("unknown")
    );
    if let Some(mut state) = auth.and_then(|auth| auth.lock().ok()) {
        state.set_token_refusal(Some(message.clone()));
    }
    refused(message)
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
        return refuse(None, "no tokens held", None, NO_TOKENS_HELD.to_string());
    };
    let session_account = auth.lock().ok().and_then(|state| state.cairn_account_id());
    let (outcome, renewed) = renew_and_adopt(host, subscriptions, auth);
    let renewed_account = renewed.as_ref().map(|(account, _)| account.as_str());
    match outcome {
        Ok(tokens) => match tokens.chatgpt_account_id {
            Some(chatgpt_account_id) => {
                if let Some((account, generation)) = &renewed {
                    log::info!(
                        "Renewed Codex tokens for Cairn account {account} (generation {generation})"
                    );
                }
                if let Ok(mut state) = auth.lock() {
                    state.set_token_refusal(None);
                }
                ServerReply::Result(json!({
                    "accessToken": tokens.access_token,
                    "chatgptAccountId": chatgpt_account_id,
                }))
            }
            None => refuse(
                Some(auth),
                "no account id",
                renewed_account.or(session_account.as_deref()),
                NO_ACCOUNT_ID.to_string(),
            ),
        },
        // The authority has already named the account and graded the failure;
        // wrapping a reconnect instruction around it would undo that.
        Err(failure) => refuse(
            Some(auth),
            &format!("renewal {}", failure.kind.as_str()),
            failure
                .account
                .as_ref()
                .map(|account| account.cairn_account_id.as_str())
                .or(renewed_account)
                .or(session_account.as_deref()),
            failure.operator_message(),
        ),
    }
}

/// Renew `auth` through [`Subscriptions::refresh_codex`] and adopt what is now
/// durable, answering with the tokens and, when the authority published, the
/// Cairn account and generation it published for the log.
fn renew_and_adopt(
    host: &dyn ModelHost,
    subscriptions: &dyn Subscriptions,
    auth: &Arc<Mutex<CodexAuthState>>,
) -> (
    Result<CodexOAuthTokens, CodexRefreshFailure>,
    Option<(String, u64)>,
) {
    let mut renewed = None;
    let outcome = refresh_session_tokens(host, auth, |request| {
        subscriptions
            .refresh_codex(host, request)
            .inspect(|(published, account)| {
                renewed = Some((
                    account.cairn_account_id.clone(),
                    codex_refresh_generation(published),
                ));
            })
    });
    (outcome, renewed)
}

/// How near its expiry a session's access token may be and still be launched
/// on.
///
/// Codex asks for fresh tokens only once the ones it holds are refused, and
/// gives up on its request after ten seconds. A token that would expire during
/// startup or a turn's first minutes is therefore renewed before launch, so
/// the session's first turn never waits on a refresh at all. ChatGPT access
/// tokens live for days, so the margin costs at most one early rotation per
/// token.
pub const LAUNCH_RENEWAL_MARGIN_SECS: i64 = 30 * 60;

/// Make the tokens a Codex app-server is about to be signed in on worth
/// presenting: if the session's access token has expired, or expires within
/// [`LAUNCH_RENEWAL_MARGIN_SECS`], renew it through
/// [`Subscriptions::refresh_codex`] first and adopt the result.
///
/// Every subscription launch passes through here before `account/login/start`.
/// An unrecoverable renewal refuses the launch: the app-server would start on
/// dead tokens and fail every request in its own words. A busy or transient
/// renewal launches on what the session holds, since Codex asks again once
/// those are refused and that attempt may succeed. A token with no readable
/// expiry is presented as it is.
///
/// Blocks while a renewal runs.
pub fn renew_for_launch(
    host: &dyn ModelHost,
    subscriptions: &dyn Subscriptions,
    auth: &Arc<Mutex<CodexAuthState>>,
) -> Result<(), CodexRefreshFailure> {
    let (expires_at, session_account) = {
        let state = auth
            .lock()
            .map_err(|_| CodexRefreshFailure::transient(None, "auth state lock poisoned"))?;
        (state.access_token_expires_at(), state.cairn_account_id())
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64);
    match expires_at {
        Some(expires_at) if expires_at - now <= LAUNCH_RENEWAL_MARGIN_SECS => {}
        _ => return Ok(()),
    }

    let (outcome, renewed) = renew_and_adopt(host, subscriptions, auth);
    match outcome {
        Ok(_) => {
            if let Some((account, generation)) = &renewed {
                log::info!(
                    "Renewed Codex tokens for Cairn account {account} (generation {generation}) before launch"
                );
            }
            Ok(())
        }
        Err(failure) => {
            let account = failure
                .account
                .as_ref()
                .map(|account| account.cairn_account_id.clone())
                .or(session_account)
                .unwrap_or_else(|| "unknown".to_string());
            let message = failure.operator_message();
            if failure.kind == CodexRefreshFailureKind::Unrecoverable {
                log::warn!(
                    "Refused a Codex launch (renewal unrecoverable) for Cairn account {account}: {message}"
                );
                Err(failure)
            } else {
                log::warn!(
                    "Launching Codex on its current tokens for Cairn account {account} (renewal {}): {message}",
                    failure.kind.as_str()
                );
                Ok(())
            }
        }
    }
}

/// What a run is told when its Codex turn fails while a token request Cairn
/// refused for this session still stands, or `None` if none does.
///
/// The app-server has already reported the failure in its own words; this is
/// the cause, in Cairn's.
pub fn standing_refusal_notice(auth: Option<&Arc<Mutex<CodexAuthState>>>) -> Option<String> {
    let refusal = auth?.lock().ok()?.token_refusal()?;
    Some(format!(
        "Cairn refused this Codex session's request for fresh ChatGPT tokens: {refusal}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use cairn_platform::services::process::{ProcessSpawner, RealProcessSpawner};

    use crate::accounts::{AccountInfo, CredentialStoreHandle, RoutedProvider};
    use crate::codex::tokens::CodexRefreshFailureKind;
    use crate::subscription::testing::RenewingCodex;
    use crate::subscription::{SignInOutcome, SignInRecordError, SignInTarget};

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

    /// Every line logged at Info or louder, by the thread that logged it, so
    /// tests running side by side each read only their own.
    mod captured {
        use std::sync::{Mutex, Once};
        use std::thread::ThreadId;

        static LINES: Mutex<Vec<(ThreadId, log::Level, String)>> = Mutex::new(Vec::new());

        struct Capture;

        impl log::Log for Capture {
            fn enabled(&self, metadata: &log::Metadata) -> bool {
                metadata.level() <= log::Level::Info
            }
            fn log(&self, record: &log::Record) {
                if self.enabled(record.metadata()) {
                    LINES.lock().unwrap().push((
                        std::thread::current().id(),
                        record.level(),
                        record.args().to_string(),
                    ));
                }
            }
            fn flush(&self) {}
        }

        /// Run `f`, answering what it returned and every line it logged.
        pub fn logged<R>(f: impl FnOnce() -> R) -> (R, Vec<(log::Level, String)>) {
            static INSTALL: Once = Once::new();
            INSTALL.call_once(|| {
                log::set_logger(&Capture)
                    .expect("the cairn-models test binary installs no other logger");
                log::set_max_level(log::LevelFilter::Info);
            });
            let me = std::thread::current().id();
            LINES.lock().unwrap().retain(|(thread, _, _)| *thread != me);
            let result = f();
            let lines = LINES
                .lock()
                .unwrap()
                .iter()
                .filter(|(thread, _, _)| *thread == me)
                .map(|(_, level, line)| (*level, line.clone()))
                .collect();
            (result, lines)
        }
    }

    /// The token text a session's credential carries; none of it may reach
    /// the log.
    const SESSION_SECRETS: [&str; 3] = ["id-1", "at-1", "rt-1"];

    fn assert_no_secrets(lines: &[(log::Level, String)]) {
        for (_, line) in lines {
            for secret in SESSION_SECRETS.iter().chain(["at-2", "id-2"].iter()) {
                assert!(!line.contains(secret), "{line} carries {secret}");
            }
        }
    }

    fn warnings(lines: &[(log::Level, String)]) -> Vec<&str> {
        lines
            .iter()
            .filter(|(level, _)| *level == log::Level::Warn)
            .map(|(_, line)| line.as_str())
            .collect()
    }

    /// A session on ChatGPT account `chatgpt-1`, holding access token `at-1`.
    fn session_auth(host: &Host) -> Arc<Mutex<CodexAuthState>> {
        let login = json!({"tokens": {
            "id_token": "id-1",
            "access_token": "at-1",
            "refresh_token": "rt-1",
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
    fn a_renewal_is_logged_with_its_account_and_generation() {
        let host = host();
        let auth = session_auth(&host);

        let (reply, lines) =
            captured::logged(|| token_request_reply(&host, &RenewingCodex::default(), Some(&auth)));

        assert!(matches!(reply, ServerReply::Result(_)));
        assert!(warnings(&lines).is_empty(), "{lines:?}");
        assert!(
            lines.iter().any(|(level, line)| *level == log::Level::Info
                && line.contains("Renewed Codex tokens")
                && line.contains("acct-cairn")
                && line.contains("generation 0")),
            "{lines:?}"
        );
        assert_no_secrets(&lines);
    }

    #[test]
    fn a_session_without_tokens_is_refused() {
        let host = host();
        let authority = RenewingCodex::default();

        let (reply, lines) = captured::logged(|| token_request_reply(&host, &authority, None));

        assert_eq!(refusal(&reply), (TOKEN_REQUEST_REFUSED, NO_TOKENS_HELD));
        assert!(authority.asked().is_empty());
        let warned = warnings(&lines);
        assert_eq!(warned.len(), 1, "{lines:?}");
        assert!(warned[0].contains("(no tokens held)"), "{}", warned[0]);
        assert!(warned[0].contains(NO_TOKENS_HELD), "{}", warned[0]);
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

    /// Each kind of failed renewal is one warning naming the kind, the
    /// account, and what Codex was told, and the refusal stands on the
    /// session for the host to report.
    #[test]
    fn every_kind_of_failed_renewal_is_logged_and_remembered() {
        for kind in [
            CodexRefreshFailureKind::Busy,
            CodexRefreshFailureKind::Transient,
            CodexRefreshFailureKind::Unrecoverable,
        ] {
            let host = host();
            let auth = session_auth(&host);
            let authority = RenewingCodex::refusing_as(kind);

            let (reply, lines) =
                captured::logged(|| token_request_reply(&host, &authority, Some(&auth)));

            let (_, message) = refusal(&reply);
            let warned = warnings(&lines);
            assert_eq!(warned.len(), 1, "{kind:?}: {lines:?}");
            let line = warned[0];
            assert!(
                line.contains(&format!("(renewal {})", kind.as_str())),
                "{line}"
            );
            assert!(line.contains("Cairn account acct-cairn"), "{line}");
            assert!(line.contains(message), "{line}");
            assert_no_secrets(&lines);

            assert_eq!(
                auth.lock().unwrap().token_refusal().as_deref(),
                Some(message)
            );
            let notice = standing_refusal_notice(Some(&auth)).expect("the refusal stands");
            assert!(notice.contains(message), "{notice}");
        }
    }

    #[test]
    fn a_renewal_clears_a_standing_refusal() {
        let host = host();
        let auth = session_auth(&host);
        token_request_reply(&host, &RenewingCodex::refusing(), Some(&auth));
        assert!(standing_refusal_notice(Some(&auth)).is_some());

        token_request_reply(&host, &RenewingCodex::default(), Some(&auth));

        assert_eq!(auth.lock().unwrap().token_refusal(), None);
        assert_eq!(standing_refusal_notice(Some(&auth)), None);
        assert_eq!(standing_refusal_notice(None), None);
    }

    /// An unsigned JWT whose `exp` is `secs_from_now` away. Codex reads
    /// nothing else of an access token at launch.
    fn access_token_expiring_in(secs_from_now: i64) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = json!({"exp": now + secs_from_now});
        format!(
            "e30.{}.at-1",
            URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes())
        )
    }

    /// A session like [`session_auth`]'s, whose access token expires
    /// `secs_from_now` from now.
    fn launching_auth(host: &Host, secs_from_now: i64) -> Arc<Mutex<CodexAuthState>> {
        let login = json!({"tokens": {
            "id_token": "id-1",
            "access_token": access_token_expiring_in(secs_from_now),
            "refresh_token": "rt-1",
            "account_id": "chatgpt-1",
        }});
        Arc::new(Mutex::new(
            CodexAuthState::new_for_account(host, &login.to_string(), Some("acct-cairn".into()))
                .unwrap(),
        ))
    }

    #[test]
    fn a_launch_on_an_expired_access_token_renews_it_first() {
        let host = host();
        let auth = launching_auth(&host, -60);
        let authority = RenewingCodex::default();

        let (outcome, lines) = captured::logged(|| renew_for_launch(&host, &authority, &auth));

        outcome.expect("the launch goes ahead on renewed tokens");
        let asked = authority.asked();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].cairn_account_id.as_deref(), Some("acct-cairn"));
        assert_eq!(asked[0].spending.as_deref(), Some("rt-1"));
        assert_eq!(auth.lock().unwrap().tokens().access_token, "at-2");
        assert!(
            lines.iter().any(|(level, line)| *level == log::Level::Info
                && line.contains("Renewed Codex tokens for Cairn account acct-cairn")
                && line.contains("before launch")),
            "{lines:?}"
        );
        assert_no_secrets(&lines);
    }

    #[test]
    fn a_launch_inside_the_renewal_margin_renews_first() {
        let host = host();
        let auth = launching_auth(&host, LAUNCH_RENEWAL_MARGIN_SECS - 60);
        let authority = RenewingCodex::default();

        renew_for_launch(&host, &authority, &auth).unwrap();

        assert_eq!(authority.asked().len(), 1);
        assert_eq!(auth.lock().unwrap().tokens().access_token, "at-2");
    }

    #[test]
    fn a_launch_on_a_live_access_token_spends_nothing() {
        let host = host();
        let auth = launching_auth(&host, 3 * 24 * 60 * 60);
        let authority = RenewingCodex::default();

        renew_for_launch(&host, &authority, &auth).unwrap();
        // A token with no readable expiry is presented as it is.
        renew_for_launch(&host, &authority, &session_auth(&host)).unwrap();

        assert!(authority.asked().is_empty());
    }

    #[test]
    fn an_unrecoverable_renewal_refuses_the_launch_with_the_operator_message() {
        let host = host();
        let auth = launching_auth(&host, -60);

        let (outcome, lines) =
            captured::logged(|| renew_for_launch(&host, &RenewingCodex::refusing(), &auth));

        let failure = outcome.expect_err("a launch on dead tokens is refused");
        let message = failure.operator_message();
        assert!(
            message.contains("Reconnect that account from Settings → Providers"),
            "{message}"
        );
        assert!(message.contains("acct-cairn"), "{message}");
        let warned = warnings(&lines);
        assert_eq!(warned.len(), 1, "{lines:?}");
        assert!(
            warned[0].contains("(renewal unrecoverable)"),
            "{}",
            warned[0]
        );
        assert_no_secrets(&lines);
    }

    /// Codex asks again once the tokens it launched on are refused, and that
    /// attempt may succeed where this one did not.
    #[test]
    fn a_busy_or_transient_renewal_launches_on_the_current_tokens() {
        for kind in [
            CodexRefreshFailureKind::Busy,
            CodexRefreshFailureKind::Transient,
        ] {
            let host = host();
            let auth = launching_auth(&host, -60);
            let before = auth.lock().unwrap().tokens().access_token;

            let (outcome, lines) = captured::logged(|| {
                renew_for_launch(&host, &RenewingCodex::refusing_as(kind), &auth)
            });

            outcome.unwrap_or_else(|failure| panic!("{kind:?} refused the launch: {failure}"));
            assert_eq!(auth.lock().unwrap().tokens().access_token, before);
            let warned = warnings(&lines);
            assert_eq!(warned.len(), 1, "{kind:?}: {lines:?}");
            assert!(
                warned[0].contains(&format!("(renewal {})", kind.as_str())),
                "{}",
                warned[0]
            );
        }
    }
}
