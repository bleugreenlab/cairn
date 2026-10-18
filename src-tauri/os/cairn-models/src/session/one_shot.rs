//! One round trip on a tool-free session: open, wait for the terminal event,
//! and return what the model answered.
//!
//! A caller opens a session with no MCP servers (see
//! [`crate::interface::session::SessionSpec::mcp`]), starts its one turn, and
//! hands the [`LaunchedSession`] here. The reader never writes a record; it is
//! the provider-neutral half of a one-shot completion, and each backend only
//! maps its reply.
//!
//! It is a fold over the session's [`SessionEvent`]s. A tool-free session that
//! tries a tool anyway is a failure, not something to answer: a
//! [`SessionEvent::ToolCall`] or a [`SessionEvent::ServerRequest`] (declined
//! first) ends the wait. Claude's own tools are refused by the CLI through the
//! caller's disallow list. The one exception is asked for by name
//! ([`OneShotAllow`]): a Codex image worker's own image tool.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::interface::session::{LaunchedSession, ProcessHandle, SessionControl, SessionEvents};
use crate::session::events::{EventReader, ReadEnd, SessionEvent, TurnStatus, STDERR_SETTLE};
use crate::wire::codex_app_server::{ImageGenerationItem, IMAGE_GENERATION_ITEM};
use crate::wire::TurnUsage;

/// How long a finished Claude process is given to exit before it is killed.
const EXIT_GRACE: Duration = Duration::from_secs(5);

/// What one tool-free round trip produced.
#[derive(Debug, Clone, PartialEq)]
pub struct OneShotReply {
    /// The final answer. For Claude with an output schema this is the
    /// CLI's summary text; the schema-valid value is [`Self::structured`].
    pub text: String,
    /// Claude's `structured_output`, when the session had an output schema.
    /// Codex returns a schema-constrained answer as [`Self::text`].
    pub structured: Option<Value>,
    /// The turn's usage in the neutral shape: `prompt_tokens` is the whole
    /// prompt, cached parts included.
    pub usage: Option<TurnUsage>,
    /// The provider's own cost figure, when it reports one.
    pub cost_usd: Option<f64>,
}

/// How a wait that let something through ended: the reply or why there is
/// none, and every image Codex's image tool returned on the way, however the
/// wait ended. An image that came before a timeout, a closed server, or a
/// forbidden tool is still here, so its saved copy can always be removed.
#[derive(Debug)]
pub struct OneShotTurn {
    pub reply: Result<OneShotReply, OneShotError>,
    pub images: Vec<ImageGenerationItem>,
}

/// What a tool-free one-shot lets through besides messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OneShotAllow {
    /// Nothing: any tool ends the wait.
    #[default]
    Nothing,
    /// Codex's image tool, on a thread opened with it
    /// ([`crate::interface::session::CodexNativeTools::ImageGeneration`]): its
    /// `imageGeneration` call goes on, and each image it returns is collected
    /// into [`OneShotTurn::images`]. Any other tool still ends the wait.
    ///
    /// Once an image has come, the turn's end answers with it whatever the
    /// turn's status, because the image item says how the image went; the
    /// reply's text is then the worker's last message, or the turn's error.
    ImageGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OneShotError {
    /// No terminal event before the deadline. The turn was stopped.
    Timeout,
    /// The provider answered with a failure, or the session tried a tool.
    Failed(String),
    /// The session ended without a terminal event, or never could deliver one.
    Protocol(String),
}

impl std::fmt::Display for OneShotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => f.write_str("one-shot session timed out"),
            Self::Failed(text) | Self::Protocol(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for OneShotError {}

const TOOL_ATTEMPT: &str = "tool-free response attempted a tool/approval";

// A token refresh is not a tool: a pooled thread's is answered by its pool,
// and one that reaches a one-shot is declined and the turn goes on.
use crate::wire::codex_app_server::AUTH_TOKENS_REFRESH_METHOD as AUTH_REFRESH;

fn kill(process: &ProcessHandle) {
    if let Ok(mut guard) = process.lock() {
        if let Some(child) = guard.as_mut() {
            let _ = child.kill();
            let _ = child.try_wait();
        }
    }
}

/// Let a process that has answered exit on its own, then make sure it has.
fn reap(process: ProcessHandle) {
    std::thread::spawn(move || {
        let deadline = Instant::now() + EXIT_GRACE;
        while Instant::now() < deadline {
            let exited = process
                .lock()
                .ok()
                .and_then(|mut guard| guard.as_mut().map(|child| child.try_wait()))
                .is_none_or(|status| !matches!(status, Ok(None)));
            if exited {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        kill(&process);
    });
}

/// Wait for `launched`'s one turn to end, at most `timeout`.
///
/// The turn must already be started: Claude in one-shot mode starts it with
/// its argument vector, and a Codex caller calls `open_turn` first. A Codex
/// session on a shared app-server has no notification channel of its own, so
/// its caller swaps in the one the pool routes that thread's messages to.
pub fn await_one_shot(
    launched: LaunchedSession,
    timeout: Duration,
) -> Result<OneShotReply, OneShotError> {
    await_one_shot_with(launched, timeout, OneShotAllow::Nothing).reply
}

/// [`await_one_shot`], letting through what `allow` names.
pub fn await_one_shot_with(
    launched: LaunchedSession,
    timeout: Duration,
    allow: OneShotAllow,
) -> OneShotTurn {
    let mut images = Vec::new();
    let reply = fold(launched, timeout, allow, &mut images);
    OneShotTurn { reply, images }
}

fn fold(
    launched: LaunchedSession,
    timeout: Duration,
    allow: OneShotAllow,
    images: &mut Vec<ImageGenerationItem>,
) -> Result<OneShotReply, OneShotError> {
    let images_allowed = allow == OneShotAllow::ImageGeneration;
    let deadline = Instant::now() + timeout;
    let LaunchedSession {
        control,
        events,
        process,
        ..
    } = launched;
    let claude = matches!(events, SessionEvents::ClaudeStream { .. });
    let mut reader = EventReader::new(events).map_err(OneShotError::Protocol)?;
    // Claude's prompt travelled as an argument; closing stdin tells a print-
    // mode CLI there is nothing more to read. Codex keeps its control to
    // interrupt the turn.
    let mut control: Option<Box<dyn SessionControl>> = if claude {
        drop(control);
        None
    } else {
        Some(control)
    };
    let stop = |control: &mut Option<Box<dyn SessionControl>>| {
        if let Some(control) = control.as_mut() {
            if let Err(error) = control.interrupt() {
                log::debug!("one-shot: interrupting the Codex turn failed: {error}");
            }
        }
    };
    let client = reader.codex_client().cloned();
    let mut outcome: Option<Result<OneShotReply, OneShotError>> = None;
    let mut interrupt = false;
    let end = reader.read(deadline, |event| {
        let (result, stop_turn) = match event {
            SessionEvent::ServerRequest { id, method, .. } if method == AUTH_REFRESH => {
                if let Some(client) = &client {
                    let _ = client.respond_error(&id, -32000, "no credential to refresh here");
                }
                return ControlFlow::Continue(());
            }
            SessionEvent::ServerRequest { id, method, .. } => {
                if let Some(client) = &client {
                    if let Err(error) = client.respond_error(&id, -32601, TOOL_ATTEMPT) {
                        log::debug!("one-shot: declining Codex {method} failed: {error}");
                    }
                }
                (
                    Err(OneShotError::Failed(format!("{TOOL_ATTEMPT} ({method})"))),
                    true,
                )
            }
            // On Codex any item other than a message is a tool, and fails the
            // wait. Claude's tools are refused by the CLI itself (the caller's
            // disallow list and `--tools ""`), and the one tool that survives
            // is `StructuredOutput`, the very way a schema-constrained answer
            // arrives; its value rides the terminal result.
            SessionEvent::ToolCall { name, .. }
                if !claude && images_allowed && name == IMAGE_GENERATION_ITEM =>
            {
                return ControlFlow::Continue(());
            }
            SessionEvent::ImageGenerated { image } if images_allowed => {
                images.push(image);
                return ControlFlow::Continue(());
            }
            SessionEvent::ToolCall { name, .. } if !claude => (
                Err(OneShotError::Failed(format!("{TOOL_ATTEMPT} ({name})"))),
                true,
            ),
            SessionEvent::ImageGenerated { .. } => (
                Err(OneShotError::Failed(format!(
                    "{TOOL_ATTEMPT} ({IMAGE_GENERATION_ITEM})"
                ))),
                true,
            ),
            SessionEvent::TurnComplete {
                status,
                text,
                structured,
                usage,
                cost_usd,
                error,
            } => {
                let result = match (status, text) {
                    (_, text) if !images.is_empty() => Ok(OneShotReply {
                        text: text.or(error).unwrap_or_default(),
                        structured,
                        usage,
                        cost_usd,
                    }),
                    (TurnStatus::Completed, Some(text)) => Ok(OneShotReply {
                        text,
                        structured,
                        usage,
                        cost_usd,
                    }),
                    (TurnStatus::Completed, None) if claude => Ok(OneShotReply {
                        text: String::new(),
                        structured,
                        usage,
                        cost_usd,
                    }),
                    (TurnStatus::Completed, None) => Err(OneShotError::Protocol(
                        "the Codex turn completed with no agent message".to_string(),
                    )),
                    _ => Err(OneShotError::Failed(error.unwrap_or_default())),
                };
                (result, false)
            }
            SessionEvent::Error {
                message,
                will_retry: false,
            } => (Err(OneShotError::Failed(message)), false),
            _ => return ControlFlow::Continue(()),
        };
        outcome = Some(result);
        interrupt = stop_turn;
        ControlFlow::Break(())
    });
    match end {
        ReadEnd::Stopped => {
            if interrupt {
                stop(&mut control);
            }
            if claude {
                reap(process);
            }
            outcome.unwrap_or_else(|| {
                Err(OneShotError::Protocol(
                    "the one-shot reader stopped with no outcome".to_string(),
                ))
            })
        }
        ReadEnd::Timeout => {
            if claude {
                kill(&process);
            } else {
                stop(&mut control);
            }
            Err(OneShotError::Timeout)
        }
        ReadEnd::Closed if claude => {
            kill(&process);
            // Give the stderr drain a moment to catch the CLI's last words.
            std::thread::sleep(STDERR_SETTLE);
            Err(OneShotError::Protocol(format!(
                "the Claude CLI exited without a result{}",
                reader.stderr_suffix()
            )))
        }
        ReadEnd::Closed => Err(OneShotError::Failed(
            "the Codex app-server closed before the turn completed".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex::app_server::AppServerClient;
    use crate::interface::session::{SessionFacts, SessionInput};
    use crate::session::events::codex::MESSAGE_ITEMS;
    use crate::wire::MessageContent;
    use cairn_platform::services::process::ChildProcess;
    use crossbeam_channel::Sender;
    use crossbeam_channel::{unbounded, Receiver};
    use serde_json::json;
    use std::io::BufRead;
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    fn facts() -> SessionFacts {
        SessionFacts {
            provider: "test".to_string(),
            backend_session_id: None,
            account_id: None,
            host: "test-host".to_string(),
            model: None,
        }
    }

    #[derive(Default)]
    struct Seen {
        interrupted: AtomicBool,
        killed: AtomicBool,
    }

    struct Control(Arc<Seen>);

    impl SessionControl for Control {
        fn open_turn(&mut self, _: &MessageContent) -> Result<(), String> {
            Ok(())
        }
        fn send(&mut self, _: &SessionInput<'_>) -> Result<(), String> {
            Ok(())
        }
        fn interrupt(&mut self) -> Result<(), String> {
            self.0.interrupted.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn set_model(&mut self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn set_permission_mode(&mut self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    struct Child(Arc<Seen>);

    impl ChildProcess for Child {
        fn id(&self) -> u32 {
            1
        }
        fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
            None
        }
        fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
            None
        }
        fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
            None
        }
        fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            Ok(None)
        }
        fn kill(&mut self) -> std::io::Result<()> {
            self.0.killed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Output that blocks until its sender is dropped.
    struct Held(Receiver<Vec<u8>>, Vec<u8>);

    impl Read for Held {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.1.is_empty() {
                match self.0.recv() {
                    Ok(bytes) => self.1 = bytes,
                    Err(_) => return Ok(0),
                }
            }
            let n = out.len().min(self.1.len());
            out[..n].copy_from_slice(&self.1[..n]);
            self.1.drain(..n);
            Ok(n)
        }
    }

    fn claude(stdout: impl Read + Send + 'static, stderr: &str) -> (LaunchedSession, Arc<Seen>) {
        let seen = Arc::new(Seen::default());
        let launched = LaunchedSession {
            control: Box::new(Control(seen.clone())),
            events: SessionEvents::ClaudeStream {
                stdout: Box::new(std::io::BufReader::new(stdout)),
                stderr: Some(Box::new(std::io::Cursor::new(stderr.as_bytes().to_vec()))),
            },
            process: Arc::new(Mutex::new(Some(Box::new(Child(seen.clone()))))),
            facts: facts(),
        };
        (launched, seen)
    }

    fn lines(events: &[Value]) -> std::io::Cursor<Vec<u8>> {
        let text: String = events.iter().map(|event| format!("{event}\n")).collect();
        std::io::Cursor::new(text.into_bytes())
    }

    fn init() -> Value {
        json!({"type": "system", "subtype": "init", "session_id": "s"})
    }

    fn result(extra: Value) -> Value {
        let mut event = json!({
            "type": "result", "subtype": "success", "session_id": "s",
            "is_error": false, "result": "the answer", "total_cost_usd": 0.0125,
            "usage": {"input_tokens": 12, "output_tokens": 5,
                      "cache_creation_input_tokens": 100, "cache_read_input_tokens": 900}
        });
        for (key, value) in extra.as_object().unwrap() {
            event[key] = value.clone();
        }
        event
    }

    const LONG: Duration = Duration::from_secs(5);

    #[test]
    fn claude_plain_text_reply() {
        let (launched, _) = claude(lines(&[init(), result(json!({}))]), "");
        let reply = await_one_shot(launched, LONG).unwrap();
        assert_eq!(reply.text, "the answer");
        assert_eq!(reply.structured, None);
        assert_eq!(reply.cost_usd, Some(0.0125));
        let usage = reply.usage.unwrap();
        // The whole prompt: 12 uncached, 100 written to cache, 900 read.
        assert_eq!(usage.prompt_tokens, Some(1012));
        assert_eq!(
            usage.prompt_tokens_details.as_ref().unwrap()["cached_tokens"],
            900
        );
        assert_eq!(usage.completion_tokens, Some(5));
    }

    #[test]
    fn claude_structured_output_rides_the_result() {
        let (launched, _) = claude(
            lines(&[
                init(),
                result(json!({"structured_output": {"answer": "yes"}})),
            ]),
            "",
        );
        let reply = await_one_shot(launched, LONG).unwrap();
        assert_eq!(reply.structured, Some(json!({"answer": "yes"})));
    }

    /// A schema-constrained Claude one-shot answers through its
    /// `StructuredOutput` tool before the result carries the value; that tool
    /// call is how the answer arrives, not a tool attempt.
    #[test]
    fn claude_structured_output_tool_call_does_not_end_the_wait() {
        let tool_use = json!({
            "type": "assistant", "uuid": "a", "session_id": "s",
            "message": {"role": "assistant", "content": [{
                "type": "tool_use", "id": "toolu_so", "name": "StructuredOutput",
                "input": {"answer": "yes"}
            }]}
        });
        let (launched, _) = claude(
            lines(&[
                init(),
                tool_use,
                result(json!({"structured_output": {"answer": "yes"}})),
            ]),
            "",
        );
        let reply = await_one_shot(launched, LONG).unwrap();
        assert_eq!(reply.structured, Some(json!({"answer": "yes"})));
    }

    #[test]
    fn claude_error_result_fails_with_its_text() {
        let (launched, _) = claude(
            lines(&[result(json!({
                "is_error": true, "subtype": "error_during_execution",
                "result": "Invalid API key"
            }))]),
            "",
        );
        assert_eq!(
            await_one_shot(launched, LONG),
            Err(OneShotError::Failed("Invalid API key".to_string()))
        );
    }

    #[test]
    fn claude_eof_without_a_result_is_a_protocol_failure_with_stderr() {
        let (launched, seen) = claude(lines(&[init()]), "not logged in\n");
        let Err(OneShotError::Protocol(text)) = await_one_shot(launched, LONG) else {
            panic!("expected a protocol failure");
        };
        assert!(text.contains("without a result"), "{text}");
        assert!(text.contains("not logged in"), "{text}");
        assert!(seen.killed.load(Ordering::SeqCst));
    }

    #[test]
    fn claude_timeout_kills_the_process() {
        let (tx, rx) = unbounded::<Vec<u8>>();
        let (launched, seen) = claude(Held(rx, Vec::new()), "");
        assert_eq!(
            await_one_shot(launched, Duration::from_millis(100)),
            Err(OneShotError::Timeout)
        );
        assert!(seen.killed.load(Ordering::SeqCst));
        drop(tx);
    }

    type Requests = Arc<Mutex<Vec<Value>>>;

    fn codex() -> (LaunchedSession, Sender<Value>, Arc<Seen>, Requests) {
        let seen = Arc::new(Seen::default());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(AppServerClient::for_test_responding(
            |_| Ok(json!({})),
            requests.clone(),
        ));
        let (tx, rx) = unbounded();
        let launched = LaunchedSession {
            control: Box::new(Control(seen.clone())),
            events: SessionEvents::CodexAppServer {
                client,
                notifications: Some(rx),
                current_turn: Arc::new(Mutex::new(None)),
            },
            process: Arc::new(Mutex::new(None)),
            facts: facts(),
        };
        (launched, tx, seen, requests)
    }

    fn notify(method: &str, params: Value) -> Value {
        let mut params = params;
        params["threadId"] = json!("thread-1");
        json!({"method": method, "params": params})
    }

    fn usage(input: u64, cached: u64, output: u64) -> Value {
        let counts = json!({"totalTokens": input + output, "inputTokens": input,
            "cachedInputTokens": cached, "outputTokens": output, "reasoningOutputTokens": 0});
        notify(
            "thread/tokenUsage/updated",
            json!({"tokenUsage": {"total": counts, "last": counts}}),
        )
    }

    fn agent_message(text: &str) -> Value {
        notify(
            "item/completed",
            json!({"item": {"id": "m", "type": "agentMessage", "text": text}}),
        )
    }

    fn turn_completed(status: &str) -> Value {
        notify(
            "turn/completed",
            json!({"turn": {"id": "turn-1", "status": status}}),
        )
    }

    #[test]
    fn codex_final_agent_message_and_accumulated_usage() {
        let (launched, tx, _, _) = codex();
        for message in [
            agent_message("draft"),
            usage(100, 80, 10),
            agent_message("final"),
            usage(160, 120, 20),
            turn_completed("completed"),
        ] {
            tx.send(message).unwrap();
        }
        let reply = await_one_shot(launched, LONG).unwrap();
        assert_eq!(reply.text, "final");
        assert_eq!(reply.cost_usd, None);
        let usage = reply.usage.unwrap();
        // The cumulative total is what the turn cost: 160 prompt tokens, 120
        // of them cached.
        assert_eq!(usage.prompt_tokens, Some(160));
        assert_eq!(
            usage.prompt_tokens_details.as_ref().unwrap()["cached_tokens"],
            120
        );
        assert_eq!(usage.completion_tokens, Some(20));
    }

    #[test]
    fn codex_failed_or_aborted_turn_fails() {
        for terminal in [
            turn_completed("failed"),
            notify("turn/aborted", json!({"reason": "interrupted"})),
            notify(
                "error",
                json!({"error": {"message": "usage limit reached"}, "willRetry": false}),
            ),
        ] {
            let (launched, tx, _, _) = codex();
            tx.send(agent_message("partial")).unwrap();
            tx.send(terminal).unwrap();
            assert!(matches!(
                await_one_shot(launched, LONG),
                Err(OneShotError::Failed(_))
            ));
        }
    }

    #[test]
    fn codex_retried_error_does_not_end_the_turn() {
        let (launched, tx, _, _) = codex();
        tx.send(notify(
            "error",
            json!({"error": {"message": "busy"}, "willRetry": true}),
        ))
        .unwrap();
        tx.send(agent_message("ok")).unwrap();
        tx.send(turn_completed("completed")).unwrap();
        assert_eq!(await_one_shot(launched, LONG).unwrap().text, "ok");
    }

    #[test]
    fn codex_server_request_is_declined_and_fails() {
        let (launched, tx, seen, requests) = codex();
        tx.send(
            json!({"id": 41, "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "thread-1"}}),
        )
        .unwrap();
        let Err(OneShotError::Failed(text)) = await_one_shot(launched, LONG) else {
            panic!("expected a failure");
        };
        assert!(text.contains("attempted a tool"), "{text}");
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0]["id"], 41);
        assert_eq!(requests[0]["error"]["code"], -32601);
        assert!(seen.interrupted.load(Ordering::SeqCst));
    }

    #[test]
    fn codex_message_items_are_allowed() {
        let (launched, tx, _, _) = codex();
        for kind in MESSAGE_ITEMS {
            tx.send(notify(
                "item/started",
                json!({"item": {"id": "i", "type": kind}}),
            ))
            .unwrap();
        }
        tx.send(agent_message("ok")).unwrap();
        tx.send(turn_completed("completed")).unwrap();
        assert_eq!(await_one_shot(launched, LONG).unwrap().text, "ok");
    }

    /// Every native tool Codex has, and one it might add, ends a tool-free
    /// turn: the reader allows messages, not a list of known tools.
    #[test]
    fn codex_tool_item_fails() {
        for kind in [
            "commandExecution",
            "mcpToolCall",
            "fileChange",
            "webSearch",
            "imageView",
            "dynamicToolCall",
            "collabAgentToolCall",
            "plan",
            "aToolCodexHasNotShippedYet",
        ] {
            let (launched, tx, seen, _) = codex();
            tx.send(notify(
                "item/started",
                json!({"item": {"id": "i", "type": kind}}),
            ))
            .unwrap();
            assert!(matches!(
                await_one_shot(launched, LONG),
                Err(OneShotError::Failed(text)) if text.contains(kind)
            ));
            assert!(seen.interrupted.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn codex_disconnect_fails() {
        let (launched, tx, _, _) = codex();
        drop(tx);
        assert!(matches!(
            await_one_shot(launched, LONG),
            Err(OneShotError::Failed(_))
        ));
    }

    #[test]
    fn codex_timeout_interrupts_the_turn() {
        let (launched, tx, seen, _) = codex();
        assert_eq!(
            await_one_shot(launched, Duration::from_millis(100)),
            Err(OneShotError::Timeout)
        );
        assert!(seen.interrupted.load(Ordering::SeqCst));
        drop(tx);
    }

    #[test]
    fn codex_completed_turn_without_a_message_is_a_protocol_failure() {
        let (launched, tx, _, _) = codex();
        tx.send(turn_completed("completed")).unwrap();
        assert!(matches!(
            await_one_shot(launched, LONG),
            Err(OneShotError::Protocol(_))
        ));
    }

    fn image_item(method: &str, status: &str, result: &str) -> Value {
        notify(
            method,
            json!({"item": {"id": "exec-1", "type": "imageGeneration", "status": status,
                "revisedPrompt": "a fox", "result": result, "failure": null,
                "savedPath": "/home/generated_images/thread-1/exec-1.png"}}),
        )
    }

    #[test]
    fn an_image_worker_s_image_is_collected_beside_its_answer() {
        let (launched, tx, seen, _) = codex();
        for message in [
            image_item("item/started", "in_progress", ""),
            image_item("item/completed", "completed", "iVBORw0KGgo="),
            agent_message("DONE"),
            usage(12_000, 11_000, 40),
            turn_completed("completed"),
        ] {
            tx.send(message).unwrap();
        }
        let turn = await_one_shot_with(launched, LONG, OneShotAllow::ImageGeneration);
        let reply = turn.reply.unwrap();
        assert_eq!(reply.text, "DONE");
        assert_eq!(turn.images.len(), 1);
        assert_eq!(turn.images[0].result, "iVBORw0KGgo=");
        assert_eq!(
            turn.images[0].saved_path.as_deref(),
            Some("/home/generated_images/thread-1/exec-1.png")
        );
        assert_eq!(reply.usage.unwrap().prompt_tokens, Some(12_000));
        assert!(!seen.interrupted.load(Ordering::SeqCst));
    }

    #[test]
    fn an_image_worker_still_fails_on_any_other_tool() {
        let (launched, tx, seen, _) = codex();
        tx.send(notify(
            "item/started",
            json!({"item": {"id": "c", "type": "commandExecution"}}),
        ))
        .unwrap();
        assert!(matches!(
            await_one_shot_with(launched, LONG, OneShotAllow::ImageGeneration).reply,
            Err(OneShotError::Failed(text)) if text.contains("commandExecution")
        ));
        assert!(seen.interrupted.load(Ordering::SeqCst));
    }

    #[test]
    fn the_image_tool_fails_a_wait_that_does_not_allow_it() {
        let (launched, tx, _, _) = codex();
        tx.send(image_item("item/started", "in_progress", ""))
            .unwrap();
        assert!(matches!(
            await_one_shot(launched, LONG),
            Err(OneShotError::Failed(text)) if text.contains("imageGeneration")
        ));
    }

    /// The item says how the image went; a turn that then fails does not
    /// hide it.
    #[test]
    fn an_image_survives_a_failed_turn() {
        let (launched, tx, _, _) = codex();
        tx.send(image_item("item/completed", "failed", "")).unwrap();
        tx.send(turn_completed("failed")).unwrap();
        let turn = await_one_shot_with(launched, LONG, OneShotAllow::ImageGeneration);
        assert_eq!(turn.images[0].status, "failed");
        assert_eq!(turn.reply.unwrap().text, "the Codex turn ended failed");
    }

    /// However the wait ends after an image came, the image is handed back,
    /// so its saved copy can be removed.
    #[test]
    fn an_image_is_handed_back_when_the_wait_fails_after_it() {
        let completed = || image_item("item/completed", "completed", "iVBORw0KGgo=");
        // Then a forbidden tool.
        let (launched, tx, _, _) = codex();
        tx.send(completed()).unwrap();
        tx.send(notify(
            "item/started",
            json!({"item": {"id": "c", "type": "commandExecution"}}),
        ))
        .unwrap();
        let turn = await_one_shot_with(launched, LONG, OneShotAllow::ImageGeneration);
        assert!(matches!(turn.reply, Err(OneShotError::Failed(_))));
        assert_eq!(turn.images.len(), 1);
        // Then nothing until the deadline.
        let (launched, tx, _, _) = codex();
        tx.send(completed()).unwrap();
        let turn = await_one_shot_with(
            launched,
            Duration::from_millis(200),
            OneShotAllow::ImageGeneration,
        );
        assert_eq!(turn.reply, Err(OneShotError::Timeout));
        assert_eq!(turn.images.len(), 1);
        drop(tx);
        // Then the server closes.
        let (launched, tx, _, _) = codex();
        tx.send(completed()).unwrap();
        drop(tx);
        let turn = await_one_shot_with(launched, LONG, OneShotAllow::ImageGeneration);
        assert!(matches!(turn.reply, Err(OneShotError::Failed(_))));
        assert_eq!(turn.images.len(), 1);
    }

    #[test]
    fn a_shared_codex_session_without_its_channel_is_refused() {
        let (mut launched, _tx, _, _) = codex();
        if let SessionEvents::CodexAppServer { notifications, .. } = &mut launched.events {
            *notifications = None;
        }
        assert!(matches!(
            await_one_shot(launched, LONG),
            Err(OneShotError::Protocol(_))
        ));
    }
}
