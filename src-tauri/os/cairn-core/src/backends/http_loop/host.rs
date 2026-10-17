//! Core's effects for the facet's `FamilyTurn`: the live transcript stream,
//! Cairn tool dispatch, and transcript persistence.
use super::*;
use cairn_models::turn::{GenerationMeta, ToolDispatch, TurnEffects};
use cairn_models::wire::replay::SKIPPED_TOOL_RESULT;
use cairn_models::wire::stream::StreamSink;
use serde_json::Value;

pub(super) struct CairnEffects<'a, A: WireAdapter> {
    pub orch: &'a Orchestrator,
    pub run_db: Arc<LocalDb>,
    pub config: SessionConfig,
    pub session_id: String,
    pub turn_id: Option<String>,
    pub adapter: A,
    pub backend_key: String,
    pub backend_name: String,
    pub model: String,
    pub context_window: Option<i64>,
}

impl<A: WireAdapter> TurnEffects for CairnEffects<'_, A> {
    fn stream_sink(&mut self) -> Box<dyn StreamSink + '_> {
        Box::new(TranscriptSink::new(
            self.orch,
            self.run_db.clone(),
            &self.config.run_id,
            &self.session_id,
            self.turn_id.as_deref(),
            &self.backend_key,
        ))
    }

    /// Dispatch the call, then persist its result unless dispatch suspended:
    /// a suspended call's real result is supplied when it resumes.
    fn execute_tool(
        &mut self,
        call: &TurnToolCall,
        truncated: bool,
    ) -> Result<ToolDispatch, String> {
        let result = execute_tool_call(self.orch, &self.config, call, truncated)?;
        if let Some(kind) = self.orch.process_state.take_suspend(&self.config.run_id) {
            return Ok(ToolDispatch::Suspend(kind));
        }
        store_tool_result(
            self.orch,
            &self.run_db,
            &self.config.run_id,
            &self.session_id,
            self.turn_id.as_deref(),
            &call.id,
            &result,
            &self.backend_key,
        )?;
        Ok(ToolDispatch::Result(render_tool_result(result)))
    }

    fn explain_error(&self, error: CompletionError) -> String {
        self.adapter.explain_error(error)
    }

    fn context_snapshot(&mut self, usage: &TurnUsage) {
        emit_context_snapshot(
            self.orch,
            &self.config.run_id,
            &self.session_id,
            &self.backend_key,
            &self.model,
            self.context_window,
            usage,
        );
    }

    fn assistant_message(&mut self, text: &str, meta: GenerationMeta<'_>) -> Result<(), String> {
        store_assistant_message(
            self.orch,
            &self.run_db,
            &self.config.run_id,
            &self.session_id,
            self.turn_id.as_deref(),
            text,
            meta.usage,
            meta.generation_id,
            meta.model,
            meta.total_cost,
            &self.backend_key,
        )
    }

    fn assistant_tool_calls(
        &mut self,
        text: &str,
        calls: &[TurnToolCall],
        meta: GenerationMeta<'_>,
        reasoning: Option<&Value>,
    ) -> Result<(), String> {
        store_assistant_tool_call(
            self.orch,
            &self.run_db,
            &self.config.run_id,
            &self.session_id,
            self.turn_id.as_deref(),
            text,
            calls,
            meta.usage,
            meta.generation_id,
            meta.model,
            reasoning,
            &self.backend_key,
        )
    }

    fn success_result(&mut self, meta: GenerationMeta<'_>) -> Result<(), String> {
        store_success_result(
            self.orch,
            &self.run_db,
            &self.config.run_id,
            &self.session_id,
            self.turn_id.as_deref(),
            meta.usage,
            meta.generation_id,
            meta.model,
            meta.total_cost,
            &self.backend_key,
        )
    }

    fn skipped_tool_result(&mut self, call_id: &str) -> Result<(), String> {
        store_tool_result(
            self.orch,
            &self.run_db,
            &self.config.run_id,
            &self.session_id,
            self.turn_id.as_deref(),
            call_id,
            &DispatchOutput {
                content: SKIPPED_TOOL_RESULT.into(),
                reminders: Vec::new(),
            },
            &self.backend_key,
        )
    }

    fn terminal_tool_armed(&self) -> bool {
        self.orch
            .process_state
            .terminal_tool_armed(&self.config.run_id)
    }

    fn capture_final_output(&mut self, text: &str) {
        match serde_json::from_str::<Value>(text) {
            Ok(value) => {
                let orch = self.orch.clone();
                let run_id = self.config.run_id.clone();
                if let Err(error) = run_backend_db(&self.backend_key, async move {
                    crate::mcp::handlers::comments_artifacts::capture_call_structured_output(
                        &orch, &run_id, value,
                    )
                    .await
                }) {
                    log::warn!(
                        "Failed to capture {} structured output for run {}: {}",
                        self.backend_name,
                        self.config.run_id,
                        error
                    );
                }
            }
            Err(error) => log::warn!(
                "{} schema-constrained call produced non-JSON content for run {}: {}",
                self.backend_name,
                self.config.run_id,
                error
            ),
        }
    }
}
