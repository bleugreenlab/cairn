use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::orchestrator::Orchestrator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SetupProgress {
    pub job_id: String,
    pub issue_id: Option<String>,
    pub kind: String,
    pub phase: Option<String>,
    pub command: Option<String>,
    pub line: Option<String>,
}

impl SetupProgress {
    pub fn new(
        job_id: impl Into<String>,
        issue_id: Option<String>,
        kind: impl Into<String>,
        phase: Option<&str>,
        command: Option<String>,
        line: Option<String>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            issue_id,
            kind: kind.into(),
            phase: phase.map(str::to_string),
            command,
            line,
        }
    }
}

pub type SetupSink = Arc<dyn Fn(SetupProgress) + Send + Sync>;

pub fn make_sink(orch: &Orchestrator, job_id: &str, issue_id: Option<String>) -> SetupSink {
    let emitter = orch.services.emitter.clone();
    let job_id = job_id.to_string();
    Arc::new(move |mut payload: SetupProgress| {
        if payload.job_id.is_empty() {
            payload.job_id = job_id.clone();
        }
        if payload.issue_id.is_none() {
            payload.issue_id = issue_id.clone();
        }
        let _ = emitter.emit("setup-progress", serde_json::json!(payload));
    })
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code)]
pub fn noop_sink() -> SetupSink {
    Arc::new(|_| {})
}

pub fn emit(
    sink: &SetupSink,
    job_id: &str,
    issue_id: Option<String>,
    kind: &str,
    phase: Option<&str>,
    command: Option<String>,
    line: Option<String>,
) {
    sink(SetupProgress::new(
        job_id.to_string(),
        issue_id,
        kind,
        phase,
        command,
        line,
    ));
}
