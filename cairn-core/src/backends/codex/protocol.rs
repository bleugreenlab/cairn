use super::app_server::AppServerClient;
use crate::agent_process::process::BackendStdin;
use crate::transcripts::stream_store::{ActiveMessageStream, StreamAccumulator};
use serde_json::Value;
use std::io::{Result as IoResult, Write};
use std::sync::{Arc, Mutex};

pub(super) struct CodexStdin {
    client: Arc<AppServerClient>,
    thread_id: String,
    cwd: String,
    model: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    current_turn_id: Arc<Mutex<Option<String>>>,
}

impl CodexStdin {
    pub(super) fn new(
        client: Arc<AppServerClient>,
        thread_id: String,
        cwd: String,
        model: Option<String>,
        reasoning_effort: Option<String>,
        service_tier: Option<String>,
        current_turn_id: Arc<Mutex<Option<String>>>,
    ) -> Self {
        Self {
            client,
            thread_id,
            cwd,
            model,
            reasoning_effort,
            service_tier,
            current_turn_id,
        }
    }

    pub(super) fn send_turn(&self, content: &str) -> Result<(), String> {
        let input = serde_json::json!([{ "type": "text", "text": content }]);
        let mut params = serde_json::json!({
            "threadId": self.thread_id,
            "cwd": self.cwd,
            "input": input,
        });
        if let Some(ref model) = self.model {
            params["model"] = serde_json::json!(model);
        }
        if let Some(ref effort) = self.reasoning_effort {
            params["reasoningEffort"] = serde_json::json!(effort);
        }
        if let Some(ref tier) = self.service_tier {
            params["serviceTier"] = serde_json::json!(tier);
        }
        let result = self.client.send_request("turn/start", params)?;
        if let Some(turn_id) = result
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(|v| v.as_str())
        {
            if let Ok(mut guard) = self.current_turn_id.lock() {
                *guard = Some(turn_id.to_string());
            }
        }
        Ok(())
    }

    pub(super) fn interrupt(&self) -> Result<(), String> {
        let Some(turn_id) = self.cached_turn_id()? else {
            return Ok(());
        };

        match self.send_interrupt_for_turn(&turn_id) {
            Ok(()) => Ok(()),
            Err(error) if is_no_active_turn_error(&error) => Ok(()),
            Err(error) => {
                if let Some(active_turn_id) = active_turn_id_from_mismatch(&error) {
                    if active_turn_id != turn_id {
                        self.remember_turn_id(&active_turn_id)?;
                        return match self.send_interrupt_for_turn(&active_turn_id) {
                            Ok(()) => Ok(()),
                            Err(retry_error) if is_no_active_turn_error(&retry_error) => Ok(()),
                            Err(retry_error) => Err(retry_error),
                        };
                    }
                }
                Err(error)
            }
        }
    }

    fn cached_turn_id(&self) -> Result<Option<String>, String> {
        let guard = self
            .current_turn_id
            .lock()
            .map_err(|e| format!("Lock poisoned: {}", e))?;
        Ok(guard.clone())
    }

    fn remember_turn_id(&self, turn_id: &str) -> Result<(), String> {
        let mut guard = self
            .current_turn_id
            .lock()
            .map_err(|e| format!("Lock poisoned: {}", e))?;
        *guard = Some(turn_id.to_string());
        Ok(())
    }

    fn send_interrupt_for_turn(&self, turn_id: &str) -> Result<(), String> {
        let params = serde_json::json!({
            "threadId": self.thread_id,
            "turnId": turn_id,
        });
        self.client.send_request("turn/interrupt", params)?;
        Ok(())
    }
}

fn json_rpc_error_message(error: &str) -> String {
    serde_json::from_str::<Value>(error)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| value.pointer("/error/message").and_then(Value::as_str))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| error.to_string())
}

fn is_no_active_turn_error(error: &str) -> bool {
    json_rpc_error_message(error).contains("no active turn to interrupt")
}

fn active_turn_id_from_mismatch(error: &str) -> Option<String> {
    let message = json_rpc_error_message(error);
    let found = message.split(" but found ").nth(1)?.trim();
    let active_turn_id = found
        .trim_matches(|ch: char| ch == '`' || ch == '\'' || ch == '"')
        .split_whitespace()
        .next()?
        .trim_matches(|ch: char| ch == '`' || ch == '\'' || ch == '"' || ch == '.' || ch == ',');
    (!active_turn_id.is_empty() && active_turn_id != "none" && active_turn_id != "null")
        .then(|| active_turn_id.to_string())
}

impl Write for CodexStdin {
    fn write(&mut self, _buf: &[u8]) -> IoResult<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "CodexStdin does not support raw writes",
        ))
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl BackendStdin for CodexStdin {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(super) struct StreamingState {
    pub(super) stream_id: String,
    pub(super) run_id: String,
    pub(super) version: i32,
    pub(super) acc: StreamAccumulator,
}

impl StreamingState {
    pub(super) fn new(stream: &ActiveMessageStream, run_id: &str) -> Self {
        Self {
            stream_id: stream.stream.id.clone(),
            run_id: run_id.to_string(),
            version: stream.stream.version,
            acc: StreamAccumulator::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[allow(clippy::type_complexity)]
    fn scripted_stdin(
        turn_id: Option<&str>,
        responses: Vec<Result<Value, String>>,
    ) -> (
        CodexStdin,
        Arc<Mutex<Vec<Value>>>,
        Arc<Mutex<Option<String>>>,
    ) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(AppServerClient::for_test_scripted(
            responses,
            requests.clone(),
        ));
        let current_turn_id = Arc::new(Mutex::new(turn_id.map(str::to_string)));
        (
            CodexStdin::new(
                client,
                "thread-1".to_string(),
                "/worktree".to_string(),
                None,
                None,
                None,
                current_turn_id.clone(),
            ),
            requests,
            current_turn_id,
        )
    }

    fn request_turn_ids(requests: &Arc<Mutex<Vec<Value>>>) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| {
                assert_eq!(
                    request.get("method").and_then(Value::as_str),
                    Some("turn/interrupt")
                );
                request
                    .pointer("/params/turnId")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn interrupt_retries_once_with_server_reported_active_turn_id() {
        let (stdin, requests, current_turn_id) = scripted_stdin(
            Some("turn-stale"),
            vec![
                Err(json!({
                    "code": -32600,
                    "message": "expected active turn id turn-stale but found turn-active"
                })
                .to_string()),
                Ok(Value::Null),
            ],
        );

        stdin.interrupt().expect("retry should land");

        assert_eq!(
            request_turn_ids(&requests),
            vec!["turn-stale".to_string(), "turn-active".to_string()]
        );
        assert_eq!(
            current_turn_id.lock().unwrap().as_deref(),
            Some("turn-active")
        );
    }

    #[test]
    fn interrupt_treats_no_active_turn_as_already_stopped() {
        let (stdin, requests, _current_turn_id) = scripted_stdin(
            Some("turn-ending"),
            vec![Err(json!({
                "code": -32600,
                "message": "no active turn to interrupt"
            })
            .to_string())],
        );

        stdin.interrupt().expect("already-stopped turn is success");

        assert_eq!(request_turn_ids(&requests), vec!["turn-ending".to_string()]);
    }

    #[test]
    fn interrupt_surfaces_unrecoverable_delivery_error() {
        let (stdin, requests, _current_turn_id) = scripted_stdin(
            Some("turn-live"),
            vec![Err(json!({
                "code": -32603,
                "message": "transport unavailable"
            })
            .to_string())],
        );

        let error = stdin
            .interrupt()
            .expect_err("unrecoverable error should surface");

        assert!(error.contains("transport unavailable"));
        assert_eq!(request_turn_ids(&requests), vec!["turn-live".to_string()]);
    }
}
