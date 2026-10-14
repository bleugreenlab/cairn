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
    token: Option<String>,
    http: reqwest::Client,
}

impl InvokeClient {
    /// Address the runner the way every other verb does: `CAIRN_CALLBACK_URL`
    /// when set (only its origin is used, so the `/api/mcp` URL an agent shell
    /// or an `ssh -L` forward carries works as-is), else the local default; the
    /// bearer token from `CAIRN_MCP_SECRET`, else this home's local secret.
    pub(crate) fn from_environment() -> Self {
        let callback = std::env::var("CAIRN_CALLBACK_URL")
            .unwrap_or_else(|_| crate::cli::default_callback_url());
        let mut parsed = url::Url::parse(&callback).expect("callback URL is valid");
        parsed.set_path("");
        parsed.set_query(None);
        parsed.set_fragment(None);
        Self {
            base_url: parsed.as_str().trim_end_matches('/').to_string(),
            token: std::env::var("CAIRN_MCP_SECRET")
                .ok()
                .or_else(cairn_common::auth::load_local_mcp_token),
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
            return Err(invoke_error_text(command, status.as_u16(), &body));
        }
        serde_json::from_str(&body).map_err(|error| format!("invalid runner response: {error}"))
    }
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
