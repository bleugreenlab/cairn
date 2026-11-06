//! The runner's `/api/invoke` command surface, as operator verbs reach it.
//!
//! `read`/`write`/`watch` speak `/api/mcp`; operator administration (`cairn
//! executor`, `cairn auth`) drives the same runner commands the desktop does, so
//! the runner stays the single implementation and each verb is one more client
//! of it.

use serde::de::DeserializeOwned;
use serde_json::{json, Value};

pub(crate) struct InvokeClient {
    base_url: String,
    /// The address came from this process's environment, so it may be its
    /// executor's relay rather than the runner.
    addressed_by_environment: bool,
    token: Option<String>,
    http: reqwest::Client,
}

impl InvokeClient {
    /// Address the runner the way every other verb does: `CAIRN_CALLBACK_URL`
    /// when set (only its origin is used, so the `/api/mcp` URL an agent shell
    /// or an `ssh -L` forward carries works as-is), else the local default; the
    /// bearer token from `CAIRN_MCP_SECRET`, else, outside any run, this home's
    /// local secret.
    pub(crate) fn from_environment() -> Self {
        let from_environment = std::env::var("CAIRN_CALLBACK_URL").ok();
        let addressed_by_environment = from_environment.is_some();
        let callback = from_environment.unwrap_or_else(crate::cli::default_callback_url);
        let mut parsed = url::Url::parse(&callback).expect("callback URL is valid");
        parsed.set_path("");
        parsed.set_query(None);
        parsed.set_fragment(None);
        Self {
            base_url: parsed.as_str().trim_end_matches('/').to_string(),
            addressed_by_environment,
            token: cairn_common::auth::callback_token(
                std::env::var("CAIRN_MCP_SECRET").ok(),
                std::env::var("CAIRN_RUN_ID").ok().as_deref(),
                cairn_common::auth::load_local_mcp_token,
            ),
            // No request timeout: some commands legitimately hold the request
            // open for minutes (a Claude sign-in code exchange, an enrollment
            // start), and each owns its own deadline on the runner side.
            http: reqwest::Client::new(),
        }
    }

    pub(crate) async fn invoke<T: DeserializeOwned>(
        &self,
        command: &str,
        args: Value,
    ) -> Result<T, String> {
        let mut request = self
            .http
            .post(format!("{}/api/invoke", self.base_url))
            .json(&json!({ "command": command, "args": args }));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|error| error.to_string())?;
        let status = response.status();
        let body = response.text().await.map_err(|error| error.to_string())?;
        if !status.is_success() {
            if self.addressed_by_environment && is_relay_answer(status.as_u16(), &body) {
                return Err(relay_refusal_text(command));
            }
            return Err(invoke_error_text(command, status.as_u16(), &body));
        }
        serde_json::from_str(&body).map_err(|error| format!("invalid runner response: {error}"))
    }
}

/// A relay's router serves only `/api/mcp`, so it answers `/api/invoke` with a bare
/// 404. The runner has the route and always says something when it refuses.
fn is_relay_answer(status: u16, body: &str) -> bool {
    status == 404 && body.trim().is_empty()
}

/// What a command that needs the runner itself says from a terminal, REPL or
/// batch, whose callback address is its executor's relay.
fn relay_refusal_text(command: &str) -> String {
    format!(
        "{command} talks to the runner directly, and this process reaches the runner only through its executor's relay, which carries MCP calls and nothing else. Run this from your own shell, outside a Cairn terminal."
    )
}

/// What a refused invoke says to the person who ran the command.
///
/// The runner's own sentence is preferred and passed through verbatim: it knows
/// which step failed and this must not paraphrase it. What this adds is the
/// floor — a failure with no message is still a failure, and `cairn executor
/// remove: ` with nothing after the colon told an operator only that something
/// went wrong twice (CAIRN-4360). An empty body is itself evidence: it means
/// the runner answered with a status and no account of it, or died mid-request,
/// and naming the status is what points at the runner log.
pub(crate) fn invoke_error_text(command: &str, status: u16, body: &str) -> String {
    let message = match serde_json::from_str::<Value>(body) {
        // A body that parsed carries the runner's answer or it carries nothing;
        // rendering the JSON itself would show an operator `{}` where a reason
        // belongs.
        Ok(value) => value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        Err(_) => body.trim().to_owned(),
    };
    if message.is_empty() {
        format!(
            "the runner refused {command} with HTTP {status} and said nothing about why. Its log is the only account of this step; check it before retrying."
        )
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_stage_specific_invoke_error_verbatim() {
        assert_eq!(
            invoke_error_text(
                "add_remote_executor",
                400,
                r#"{"error":"remote prerequisite preflight failed: binary missing"}"#
            ),
            "remote prerequisite preflight failed: binary missing"
        );
    }

    #[test]
    fn a_relay_s_bare_404_is_told_apart_from_the_runner_s_refusal() {
        assert!(is_relay_answer(404, ""));
        assert!(!is_relay_answer(404, r#"{"error":"no such command"}"#));
        assert!(!is_relay_answer(500, ""));
        let text = relay_refusal_text("add_remote_executor");
        assert!(
            text.contains("add_remote_executor") && text.contains("relay"),
            "{text}"
        );
    }

    /// A failure that carries no message still has to say something. An empty
    /// body is what produced `cairn executor remove: ` with nothing after it,
    /// which is indistinguishable from the command having succeeded quietly.
    #[test]
    fn a_failure_with_no_message_still_names_the_command_and_the_status() {
        for body in ["", "   ", "{}", r#"{"error":""}"#] {
            let text = invoke_error_text("remove_remote_executor", 500, body);
            assert!(text.contains("remove_remote_executor"), "{text}");
            assert!(text.contains("500"), "{text}");
        }
    }
}
