//! Shared `#[cfg(test)]` helpers used across the module test suites.
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cairn_common::protocol::CallbackRequest;
use rmcp::model::CallToolResult;

use crate::output::CallbackOutcome;
use crate::schemas::RunInput;
use crate::server::{CairnMcpServer, Caller};
use crate::sink::CallbackSink;

/// A sink that answers every call with one canned outcome and records each
/// request it was handed.
pub(crate) struct RecordingSink {
    outcome: CallbackOutcome,
    requests: Mutex<Vec<CallbackRequest>>,
}

impl RecordingSink {
    pub(crate) fn answering(outcome: CallbackOutcome) -> Arc<Self> {
        Arc::new(Self {
            outcome,
            requests: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn requests(&self) -> Vec<CallbackRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl CallbackSink for RecordingSink {
    async fn call(&self, request: &CallbackRequest) -> CallbackOutcome {
        self.requests.lock().unwrap().push(request.clone());
        self.outcome.clone()
    }

    fn describe(&self) -> &str {
        "recording sink"
    }
}

/// A fixed-run server over a sink that answers every call with an empty
/// success, for tests that exercise the server's own logic.
pub(crate) fn test_server() -> CairnMcpServer {
    CairnMcpServer::new(
        RecordingSink::answering(CallbackOutcome {
            transport_ok: true,
            ..Default::default()
        }),
        Caller::Fixed {
            run_id: None,
            cwd: "/test/path".to_string(),
        },
    )
}

pub(crate) fn get_text(result: &CallToolResult) -> &str {
    result
        .content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.as_ref())
        .unwrap()
}

pub(crate) fn run_input(value: serde_json::Value) -> RunInput {
    serde_json::from_value(value).expect("valid RunInput")
}
