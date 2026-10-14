//! OpenCode Go's endpoint and authentication facts.
//!
//! Go fronts three protocol families behind one base URL, and this module is
//! only where each one lives and how it authenticates. The request bodies and
//! stream parsers belong to their protocol modules (`openai_compat`,
//! `anthropic_messages`, `openai_responses`), which know nothing about OpenCode.

use super::{OPENCODE_BACKEND_KEY, OPENCODE_BACKEND_NAME};
use crate::backends::anthropic_messages::http::MessagesEndpoint;
use crate::backends::openai_compat::http::Endpoint;
use crate::backends::openai_responses::http::ResponsesEndpoint;

pub(super) const CHAT_URL: &str = "https://opencode.ai/zen/go/v1/chat/completions";
pub(super) const MESSAGES_URL: &str = "https://opencode.ai/zen/go/v1/messages";
pub(super) const RESPONSES_URL: &str = "https://opencode.ai/zen/go/v1/responses";

/// The `max_tokens` every Messages request carries.
///
/// That protocol REQUIRES the field — omitting it against this gateway is an
/// upstream 500, not a default — and the value is a per-turn output cap rather
/// than a budget the subscription meters, so it is set generously enough not to
/// truncate a real coding turn.
const MESSAGES_MAX_OUTPUT_TOKENS: i64 = 32_000;

pub(super) fn endpoint(api_key: &str) -> Endpoint {
    Endpoint {
        provider_name: OPENCODE_BACKEND_NAME.to_string(),
        backend_key: OPENCODE_BACKEND_KEY.to_string(),
        chat_url: CHAT_URL.to_string(),
        headers: vec![
            ("authorization".to_string(), format!("Bearer {api_key}")),
            ("content-type".to_string(), "application/json".to_string()),
        ],
    }
}

/// Go's Messages endpoint authenticates with Anthropic's own `x-api-key` header,
/// NOT the Bearer token its other two endpoints take.
///
/// Verified against the live gateway on 2026-08-16: the identical request
/// authenticated with `Authorization: Bearer` is refused with `401 AuthError:
/// Missing API key`, while `x-api-key` succeeds. `anthropic-version` is accepted
/// but not required, so it is not sent.
pub(super) fn messages_endpoint(api_key: &str) -> MessagesEndpoint {
    MessagesEndpoint {
        provider_name: OPENCODE_BACKEND_NAME.to_string(),
        backend_key: OPENCODE_BACKEND_KEY.to_string(),
        url: MESSAGES_URL.to_string(),
        headers: vec![
            ("x-api-key".to_string(), api_key.to_string()),
            ("content-type".to_string(), "application/json".to_string()),
        ],
        max_output_tokens: MESSAGES_MAX_OUTPUT_TOKENS,
    }
}

pub(super) fn responses_endpoint(api_key: &str) -> ResponsesEndpoint {
    ResponsesEndpoint {
        provider_name: OPENCODE_BACKEND_NAME.to_string(),
        backend_key: OPENCODE_BACKEND_KEY.to_string(),
        url: RESPONSES_URL.to_string(),
        headers: vec![
            ("authorization".to_string(), format!("Bearer {api_key}")),
            ("content-type".to_string(), "application/json".to_string()),
        ],
    }
}
