//! The provider-neutral HTTP agent turn: one user turn's loop of generation,
//! tool dispatch, and the next generation, until the model answers without a
//! tool call.
//!
//! The driver decides only *when* to request another generation. Everything
//! with an effect is the host's, through [`HttpTurnHost`]: building and posting
//! the request (with whatever tools the host advertises in
//! [`crate::wire::GenerationConfig::tools`]), keeping the native conversation,
//! persisting assistant output, and executing each tool call. So a host outside
//! Cairn runs the same loop Cairn's own HTTP backends do, with its own tools
//! and its own storage.
//!
//! The driver reports how the loop ended as a [`TurnExit`]. Whether a
//! transport error or a panic fails a run is host policy, so neither is a
//! variant: an error is the `Err` of [`run_turn`], and a host that wants panic
//! containment wraps the call itself.
//!
//! Most hosts do not implement [`HttpTurnHost`] at all. [`FamilyTurn`] is the
//! standard one: it pairs a [`ProtocolFamily`] (chat completions, Anthropic
//! Messages, or OpenAI Responses), which owns the request and the native
//! conversation, with the host's [`TurnEffects`], which own the stream, the
//! tools, and persistence.
mod family;
mod host;

pub use family::{ChatFamily, MessagesFamily, ProtocolFamily, ResponsesFamily};
pub use host::{FamilyTurn, ToolDispatch, TurnEffects};

use crate::wire::{Generation, TurnToolCall, TurnUsage};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};

/// Runaway guard, not a turn-length limit: a real agent turn can legitimately
/// exceed dozens of tool round-trips, so the cap is set high and the turn
/// finalizes gracefully on reaching it rather than failing.
pub const MAX_TOOL_ITERATIONS: usize = 200;

/// Why a turn yields to the host instead of issuing another request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuspendKind {
    /// A foreground question was asked; resume when the user answers.
    Prompt,
    /// An inline (non-background) delegated task was spawned; resume when the
    /// child task(s) complete.
    DelegatedTask,
}

/// How [`run_turn`] ended. Distinct from a provider's own result: a durable
/// wait can end the loop with neither a success nor a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnExit {
    /// The model answered without a tool call (or a terminal tool armed, or the
    /// iteration budget ran out), and the host's `success_result` was recorded.
    Completed,
    /// The cancel flag was observed between steps.
    Cancelled,
    /// A tool call asked to wait; the host resumes the conversation later.
    Suspended(SuspendKind),
}

/// Provider-neutral facts about one generation, passed to the host's
/// persistence effects.
#[derive(Clone, Copy, Default)]
pub struct GenerationMeta<'a> {
    pub usage: Option<&'a TurnUsage>,
    pub generation_id: Option<&'a str>,
    pub model: Option<&'a str>,
    /// The exact cost summed over this turn's generations so far, when the
    /// provider reports one.
    pub total_cost: Option<f64>,
}

pub struct HttpTurnConfig<'a> {
    /// Names the backend in diagnostics only.
    pub backend_name: &'a str,
    pub model: &'a str,
    /// Whether the final answer is structured output the host captures.
    pub has_output_schema: bool,
}

/// Host effects for one turn. The host's request builder retains the native
/// replay messages; the driver receives only provider-neutral turn facts.
///
/// Every effect takes `&mut self`, so a host that keeps its conversation or
/// event log in plain fields needs no interior mutability.
pub trait HttpTurnHost {
    /// Fit, build, and post one generation request, appending the assistant's
    /// native message to the host's conversation.
    fn post_generation(&mut self) -> Result<Generation<()>, String>;
    /// The generation's usage, as soon as it is known (a live context gauge).
    fn context_snapshot(&mut self, usage: &TurnUsage);
    /// Persist a final assistant message that was not already streamed.
    fn assistant_message(&mut self, text: &str, meta: GenerationMeta<'_>) -> Result<(), String>;
    /// Persist an assistant message that carries tool calls.
    fn assistant_tool_calls(
        &mut self,
        text: &str,
        calls: &[TurnToolCall],
        meta: GenerationMeta<'_>,
        reasoning: Option<&Value>,
    ) -> Result<(), String>;
    /// Record that the turn succeeded.
    fn success_result(&mut self, meta: GenerationMeta<'_>) -> Result<(), String>;
    /// Execute, then persist and append the native tool result unless dispatch
    /// suspended. A suspended call's real result is supplied when it resumes.
    /// `truncated` marks the last call of a generation cut off by its output
    /// cap, whose arguments may be incomplete.
    fn execute_tool(
        &mut self,
        call: &TurnToolCall,
        truncated: bool,
    ) -> Result<Option<SuspendKind>, String>;
    /// Pair a call left unexecuted by a suspension, so a resumed
    /// OpenAI-style replay stays valid.
    fn skipped_tool_result(&mut self, call_id: &str) -> Result<(), String>;
    /// Whether an executed tool ended the turn (a final-answer tool).
    fn terminal_tool_armed(&self) -> bool;
    /// Capture the final answer as structured output.
    fn capture_final_output(&mut self, text: &str);
}

pub fn run_turn(
    host: &mut dyn HttpTurnHost,
    config: HttpTurnConfig<'_>,
    cancel: &AtomicBool,
) -> Result<TurnExit, String> {
    let mut exact_cost = None;
    for _ in 0..MAX_TOOL_ITERATIONS {
        if cancel.load(Ordering::SeqCst) {
            return Ok(TurnExit::Cancelled);
        }
        let generation = host.post_generation()?;
        // A cancelled SSE request may have returned a partial generation.
        if cancel.load(Ordering::SeqCst) {
            return Ok(TurnExit::Cancelled);
        }
        let Generation {
            assistant_message: (),
            assistant_text,
            tool_calls,
            reasoning_details,
            usage,
            finish_reason,
            generation_id,
            response_model,
            streamed_text,
        } = generation;
        let truncated = finish_reason.as_deref() == Some("length");
        if let Some(cost) = usage.as_ref().and_then(|usage| usage.cost) {
            exact_cost = Some(exact_cost.unwrap_or(0.0) + cost);
        }
        if let Some(usage) = usage.as_ref() {
            host.context_snapshot(usage);
        }
        if truncated {
            log::warn!(
                "{} generation truncated (finish_reason=length) model={} tool={}",
                config.backend_name,
                config.model,
                tool_calls
                    .last()
                    .map(|call| call.name.as_str())
                    .unwrap_or("<none>")
            );
        }
        let meta = GenerationMeta {
            usage: usage.as_ref(),
            generation_id: generation_id.as_deref(),
            model: response_model.as_deref(),
            total_cost: exact_cost,
        };
        if tool_calls.is_empty() {
            if !assistant_text.is_empty() && !streamed_text {
                host.assistant_message(&assistant_text, meta)?;
            }
            host.success_result(meta)?;
            if config.has_output_schema && !assistant_text.is_empty() {
                host.capture_final_output(&assistant_text);
            }
            return Ok(TurnExit::Completed);
        }
        host.assistant_tool_calls(
            if streamed_text { "" } else { &assistant_text },
            &tool_calls,
            GenerationMeta {
                usage: tool_call_usage(streamed_text, usage.as_ref()),
                ..meta
            },
            reasoning_details.as_ref(),
        )?;
        for (index, call) in tool_calls.iter().enumerate() {
            if let Some(kind) =
                host.execute_tool(call, truncated && index + 1 == tool_calls.len())?
            {
                // Pair the unexecuted calls so resumed OpenAI-style replay is valid.
                for skipped in tool_calls.iter().skip(index + 1) {
                    host.skipped_tool_result(&skipped.id)?;
                }
                return Ok(TurnExit::Suspended(kind));
            }
        }
        if host.terminal_tool_armed() {
            host.success_result(meta)?;
            return Ok(TurnExit::Completed);
        }
        if cancel.load(Ordering::SeqCst) {
            return Ok(TurnExit::Cancelled);
        }
    }
    let meta = GenerationMeta {
        total_cost: exact_cost,
        ..GenerationMeta::default()
    };
    host.assistant_message(&format!("Reached the per-turn tool-iteration budget ({MAX_TOOL_ITERATIONS}); finalizing this turn."), meta)?;
    host.success_result(meta)?;
    Ok(TurnExit::Completed)
}

/// The usage to attach to a tool-call assistant event. A streamed generation's
/// usage is already recorded on its finalized streamed message, so attaching it
/// again would double-count.
pub fn tool_call_usage(streamed_text: bool, usage: Option<&TurnUsage>) -> Option<&TurnUsage> {
    if streamed_text {
        None
    } else {
        usage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::VecDeque;

    #[derive(Default)]
    struct Host {
        generations: VecDeque<Generation<()>>,
        trace: RefCell<Vec<String>>,
        suspend: Option<SuspendKind>,
        terminal: bool,
        fail_persist: bool,
    }
    impl Host {
        fn record(&self, value: impl Into<String>) {
            self.trace.borrow_mut().push(value.into());
        }
    }
    impl HttpTurnHost for Host {
        fn post_generation(&mut self) -> Result<Generation<()>, String> {
            self.record("post");
            self.generations
                .pop_front()
                .ok_or_else(|| "unexpected extra request".into())
        }
        fn context_snapshot(&mut self, _: &TurnUsage) {
            self.record("usage");
        }
        fn assistant_message(&mut self, _: &str, _: GenerationMeta<'_>) -> Result<(), String> {
            self.record("assistant");
            if self.fail_persist {
                Err("transcript unavailable".into())
            } else {
                Ok(())
            }
        }
        fn assistant_tool_calls(
            &mut self,
            _: &str,
            _: &[TurnToolCall],
            meta: GenerationMeta<'_>,
            _: Option<&Value>,
        ) -> Result<(), String> {
            self.record(if meta.usage.is_some() {
                "calls-with-usage"
            } else {
                "calls"
            });
            Ok(())
        }
        fn success_result(&mut self, _: GenerationMeta<'_>) -> Result<(), String> {
            self.record("success");
            Ok(())
        }
        fn execute_tool(
            &mut self,
            call: &TurnToolCall,
            truncated: bool,
        ) -> Result<Option<SuspendKind>, String> {
            self.record(format!("execute:{}:{truncated}", call.id));
            Ok(self.suspend.take())
        }
        fn skipped_tool_result(&mut self, id: &str) -> Result<(), String> {
            self.record(format!("skip:{id}"));
            Ok(())
        }
        fn terminal_tool_armed(&self) -> bool {
            self.terminal
        }
        fn capture_final_output(&mut self, _: &str) {
            self.record("capture");
        }
    }
    fn generation(calls: usize) -> Generation<()> {
        Generation {
            assistant_message: (),
            assistant_text: "final answer".into(),
            tool_calls: (0..calls)
                .map(|n| TurnToolCall {
                    id: n.to_string(),
                    name: "read".into(),
                    arguments: "{}".into(),
                })
                .collect(),
            reasoning_details: None,
            usage: None,
            finish_reason: None,
            generation_id: None,
            response_model: None,
            streamed_text: false,
        }
    }
    fn drive(host: &mut Host, cancelled: bool) -> Result<TurnExit, String> {
        run_turn(
            host,
            HttpTurnConfig {
                backend_name: "fixture",
                model: "fixture",
                has_output_schema: true,
            },
            &AtomicBool::new(cancelled),
        )
    }
    #[test]
    fn completed_generation_persists_before_structured_capture() {
        let mut host = Host {
            generations: [generation(0)].into(),
            ..Host::default()
        };
        assert_eq!(drive(&mut host, false).unwrap(), TurnExit::Completed);
        assert_eq!(
            *host.trace.borrow(),
            ["post", "assistant", "success", "capture"]
        );
    }
    #[test]
    fn persistence_failure_never_reports_success_or_captures_output() {
        let mut host = Host {
            generations: [generation(0)].into(),
            fail_persist: true,
            ..Host::default()
        };
        assert_eq!(
            drive(&mut host, false).unwrap_err(),
            "transcript unavailable"
        );
        assert_eq!(*host.trace.borrow(), ["post", "assistant"]);
    }
    #[test]
    fn suspension_pairs_unexecuted_calls_without_reporting_success() {
        let mut host = Host {
            generations: [generation(3)].into(),
            suspend: Some(SuspendKind::DelegatedTask),
            ..Host::default()
        };
        assert_eq!(
            drive(&mut host, false).unwrap(),
            TurnExit::Suspended(SuspendKind::DelegatedTask)
        );
        assert_eq!(
            *host.trace.borrow(),
            ["post", "calls", "execute:0:false", "skip:1", "skip:2"]
        );
    }
    #[test]
    fn only_last_call_in_length_capped_generation_is_suspected_truncated() {
        let mut first = generation(2);
        first.finish_reason = Some("length".into());
        let mut host = Host {
            generations: [first].into(),
            terminal: true,
            ..Host::default()
        };
        assert_eq!(drive(&mut host, false).unwrap(), TurnExit::Completed);
        assert_eq!(
            *host.trace.borrow(),
            [
                "post",
                "calls",
                "execute:0:false",
                "execute:1:true",
                "success"
            ]
        );
    }
    #[test]
    fn cancellation_prevents_even_the_first_request() {
        let mut host = Host::default();
        assert_eq!(drive(&mut host, true).unwrap(), TurnExit::Cancelled);
        assert!(host.trace.borrow().is_empty());
    }
}
