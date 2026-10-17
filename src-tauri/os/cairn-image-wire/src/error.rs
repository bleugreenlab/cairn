//! The one error every image wire returns.

use serde_json::Value;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ImageWireError {
    #[error("image request timed out")]
    Timeout,
    /// The request could not be built (a bad header, an unencodable form).
    #[error("image request is invalid: {0}")]
    InvalidRequest(String),
    /// The request never got an HTTP answer.
    #[error("{0}")]
    Transport(String),
    /// The provider answered with an error status. `code` is the provider's
    /// own error code when its body carried one (`moderation_blocked`,
    /// OpenRouter's numeric `402`), `message` its sentence or else the body.
    #[error("HTTP {status}: {message}")]
    Http {
        status: u16,
        code: Option<String>,
        message: String,
    },
    #[error("image reply was invalid: {0}")]
    InvalidResponse(String),
}

impl ImageWireError {
    /// An error status with `body`, read as `{"error": {"code", "message"}}`
    /// when it is that shape and kept verbatim when it is not: an
    /// unrecognised body is still evidence.
    pub fn from_status(status: u16, body: &str) -> Self {
        let parsed = serde_json::from_str::<Value>(body).ok();
        let error = parsed
            .as_ref()
            .map(|parsed| parsed.get("error").unwrap_or(parsed));
        let code = error.and_then(|error| match error.get("code")? {
            Value::String(code) => Some(code.clone()),
            Value::Number(code) => Some(code.to_string()),
            _ => None,
        });
        let message = error
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str)
            .filter(|message| !message.trim().is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| body.to_string());
        Self::Http {
            status,
            code,
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_error_body_yields_its_code_and_sentence_and_anything_else_is_kept() {
        assert_eq!(
            ImageWireError::from_status(
                400,
                r#"{"error":{"message":"Your request was rejected by the safety system.","type":"image_generation_user_error","param":null,"code":"moderation_blocked"}}"#
            ),
            ImageWireError::Http {
                status: 400,
                code: Some("moderation_blocked".into()),
                message: "Your request was rejected by the safety system.".into(),
            }
        );
        assert_eq!(
            ImageWireError::from_status(
                402,
                r#"{"error":{"code":402,"message":"Insufficient credits. Add more using https://openrouter.ai/credits"}}"#
            ),
            ImageWireError::Http {
                status: 402,
                code: Some("402".into()),
                message: "Insufficient credits. Add more using https://openrouter.ai/credits"
                    .into(),
            }
        );
        assert_eq!(
            ImageWireError::from_status(502, "<html>Bad Gateway</html>"),
            ImageWireError::Http {
                status: 502,
                code: None,
                message: "<html>Bad Gateway</html>".into(),
            }
        );
    }
}
