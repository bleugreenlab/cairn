//! Fallible live-output boundary. The caller owns storage and presentation.
use crate::TurnUsage;
use serde_json::Value;

/// The final snapshot of an opened text/reasoning stream. Finalizing a partial
/// snapshot does not mean the generation completed successfully.
#[derive(Debug)]
pub struct StreamSummary {
    pub text: String,
    pub thinking: String,
    pub reasoning_details: Vec<Value>,
    pub usage: Option<TurnUsage>,
    pub generation_id: Option<String>,
    pub model: Option<String>,
}

/// Called synchronously in wire order. A sink error stops the generation;
/// callers must not acknowledge output they could not record.
pub trait StreamSink {
    fn append(&mut self, text: &str) -> Result<(), String>;
    fn append_thinking(&mut self, text: &str) -> Result<(), String>;
    fn finalize(&mut self, summary: StreamSummary) -> Result<(), String>;
}
