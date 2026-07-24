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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_id: Option<String>,
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
            elapsed_ms: None,
            store_id: None,
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

/// Emit a machine-readable phase duration correlated by the canonical store path.
pub fn emit_timing(
    sink: &SetupSink,
    job_id: &str,
    issue_id: Option<String>,
    phase: &str,
    elapsed_ms: u64,
    store_id: String,
) {
    let mut progress = SetupProgress::new(job_id, issue_id, "timing", Some(phase), None, None);
    progress.elapsed_ms = Some(elapsed_ms);
    progress.store_id = Some(store_id);
    sink(progress);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timing_progress_is_machine_readable_and_store_correlated() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = events.clone();
        let sink: SetupSink = Arc::new(move |event| captured.lock().unwrap().push(event));
        emit_timing(
            &sink,
            "job-1",
            Some("issue-1".into()),
            "populate-discovery-copy",
            12_345,
            "/stores/project".into(),
        );
        let event = events.lock().unwrap().pop().unwrap();
        assert_eq!(event.kind, "timing");
        assert_eq!(event.phase.as_deref(), Some("populate-discovery-copy"));
        assert_eq!(event.elapsed_ms, Some(12_345));
        assert_eq!(event.store_id.as_deref(), Some("/stores/project"));
        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["elapsedMs"], 12_345);
        assert_eq!(json["storeId"], "/stores/project");
    }
}
