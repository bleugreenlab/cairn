//! The stdio server's route to the host: each tool call is POSTed to the
//! runner's `/api/mcp` callback with the machine's MCP secret.
use std::sync::Arc;

use async_trait::async_trait;
use cairn_common::protocol::{CallbackRequest, CallbackResponse};
use cairn_mcp_server::output::{http_status_error_message, truncate_chars};
use cairn_mcp_server::{CairnMcpServer, CallbackOutcome, CallbackSink, Caller};

use crate::timeouts::callback_timeout;

/// The server one `cairn-cmd` process speaks through: a fixed run identity
/// from its environment, with every call forwarded over the HTTP callback.
pub(crate) fn fixed_server(
    callback_url: String,
    cwd: String,
    run_id: Option<String>,
    mcp_secret: Option<String>,
) -> CairnMcpServer {
    CairnMcpServer::new(
        Arc::new(HttpCallbackSink {
            callback_url,
            mcp_secret,
        }),
        Caller::Fixed { run_id, cwd },
    )
}

struct HttpCallbackSink {
    callback_url: String,
    /// Shared secret (base64-encoded string from env var, sent directly as bearer token)
    mcp_secret: Option<String>,
}

#[async_trait]
impl CallbackSink for HttpCallbackSink {
    fn describe(&self) -> &str {
        &self.callback_url
    }

    async fn call(&self, request: &CallbackRequest) -> CallbackOutcome {
        let client = match reqwest::Client::builder()
            .timeout(callback_timeout(request))
            .build()
        {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("Failed to build HTTP client: {}", e);
                return CallbackOutcome {
                    result: format!("Error building HTTP client: {}", e),
                    ..Default::default()
                };
            }
        };
        let mut req = client.post(self.callback_url.as_str()).json(request);

        if let Some(secret) = &self.mcp_secret {
            req = req.header("Authorization", format!("Bearer {}", secret));
        }

        let request_bytes = serde_json::to_vec(request).map(|v| v.len()).unwrap_or(0);
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let http_ok = status.is_success();
                match resp.text().await {
                    // A non-2xx callback (e.g. HTTP 413 when the body exceeds the
                    // callback limit) is not a `CallbackResponse`; surface the
                    // status and request size explicitly instead of failing to
                    // parse the error body into an opaque message.
                    Ok(text) if !http_ok => {
                        tracing::error!(
                            "MCP callback returned HTTP {} for tool {}: {}",
                            status,
                            request.tool,
                            truncate_chars(&text, 500)
                        );
                        CallbackOutcome {
                            result: http_status_error_message(
                                status.as_u16(),
                                status.canonical_reason().unwrap_or("error"),
                                request_bytes,
                            ),
                            ..Default::default()
                        }
                    }
                    Ok(text) => match serde_json::from_str::<CallbackResponse>(&text) {
                        Ok(r) => CallbackOutcome {
                            result: r.result,
                            reminders: r.reminders,
                            transport_ok: http_ok,
                        },
                        Err(e) => {
                            tracing::error!(
                                "Failed to parse response (status {}): {} - body: {}",
                                status,
                                e,
                                truncate_chars(&text, 500)
                            );
                            CallbackOutcome {
                                result: format!(
                                    "Error parsing response: {} (body: {})",
                                    e,
                                    truncate_chars(&text, 200)
                                ),
                                ..Default::default()
                            }
                        }
                    },
                    Err(e) => CallbackOutcome {
                        result: format!("Error reading response body: {}", e),
                        ..Default::default()
                    },
                }
            }
            Err(e) => CallbackOutcome {
                result: format!("Error calling Tauri: {}", e),
                ..Default::default()
            },
        }
    }
}
