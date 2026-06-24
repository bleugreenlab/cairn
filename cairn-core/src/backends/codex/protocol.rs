use super::app_server::AppServerClient;
use crate::agent_process::process::BackendStdin;
use crate::transcripts::stream_store::{ActiveMessageStream, StreamAccumulator};
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
        let turn_id = {
            let guard = self
                .current_turn_id
                .lock()
                .map_err(|e| format!("Lock poisoned: {}", e))?;
            guard.clone()
        };
        if let Some(turn_id) = turn_id {
            let params = serde_json::json!({
                "threadId": self.thread_id,
                "turnId": turn_id,
            });
            self.client.send_request("turn/interrupt", params)?;
        }
        Ok(())
    }
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
