//! The one writer to a live Codex thread: its turns, interrupts, and native goal.

use crate::codex::app_server::AppServerClient;
use crate::interface::session::{ServerReply, SessionControl, SessionInput};
#[cfg(test)]
use crate::wire::codex_app_server::goal::GoalClearResponse;
use crate::wire::codex_app_server::goal::{parse_response, Goal, GoalResponse, GoalStatus};
use crate::wire::MessageContent;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const WATCHDOG_INTERRUPT_ACK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactTurnInterrupt {
    AlreadyTerminal {
        terminal_observed_at_ms: i64,
    },
    Acknowledged {
        interrupt_response_at_ms: i64,
        terminal_observed_at_ms: i64,
    },
}

fn unix_time_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub struct CodexControl {
    client: Arc<AppServerClient>,
    thread_id: String,
    cwd: String,
    model: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    current_turn_id: Arc<Mutex<Option<String>>>,
    /// What only the thread's first turn carries.
    opening: Opening,
}

#[derive(Default)]
struct Opening {
    /// Constrain the final agent message to this schema, natively.
    output_schema: Option<Value>,
    /// Replaces the first turn's text when the thread was rebuilt from a
    /// transcript because the one it resumed was gone.
    text: Option<String>,
}

impl CodexControl {
    pub fn new(
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
            opening: Opening::default(),
        }
    }

    /// What the first turn carries beyond its message: an output schema, and
    /// the transcript-preload prompt when the thread was rebuilt.
    pub fn with_opening(mut self, output_schema: Option<Value>, text: Option<String>) -> Self {
        self.opening = Opening {
            output_schema,
            text,
        };
        self
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub fn interrupt_untracked_turn(&self, turn_id: &str) -> Result<(), String> {
        match self.send_interrupt_for_turn(turn_id) {
            Ok(()) => Ok(()),
            Err(error) if is_no_active_turn_error(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn interrupt_exact_and_wait(
        &self,
        expected_turn_id: &str,
    ) -> Result<ExactTurnInterrupt, String> {
        match self.cached_turn_id()? {
            None => {
                return Ok(ExactTurnInterrupt::AlreadyTerminal {
                    terminal_observed_at_ms: unix_time_ms(),
                })
            }
            Some(turn_id) if turn_id != expected_turn_id => {
                return Err(format!(
                    "active Codex turn mismatch: expected {expected_turn_id}, found {turn_id}"
                ));
            }
            Some(_) => {}
        }
        match self.send_interrupt_for_turn(expected_turn_id) {
            Ok(()) => {}
            Err(error) if is_no_active_turn_error(&error) => {
                return Ok(ExactTurnInterrupt::AlreadyTerminal {
                    terminal_observed_at_ms: unix_time_ms(),
                });
            }
            Err(error) => {
                return Err(format!(
                    "interrupt delivery failed at {}ms: {error}",
                    unix_time_ms()
                ))
            }
        }
        let interrupt_response_at_ms = unix_time_ms();
        let deadline = Instant::now() + WATCHDOG_INTERRUPT_ACK_TIMEOUT;
        loop {
            match self.cached_turn_id()? {
                None => {
                    return Ok(ExactTurnInterrupt::Acknowledged {
                        interrupt_response_at_ms,
                        terminal_observed_at_ms: unix_time_ms(),
                    })
                }
                Some(turn_id) if turn_id != expected_turn_id => {
                    return Err(format!(
                        "active Codex turn mismatch after interrupt: expected {expected_turn_id}, found {turn_id}"
                    ));
                }
                Some(_) => {}
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "interrupt acknowledgement timed out after {}ms; interrupt_response_at_ms={}; timeout_observed_at_ms={}",
                    WATCHDOG_INTERRUPT_ACK_TIMEOUT.as_millis(),
                    interrupt_response_at_ms,
                    unix_time_ms()
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn get_goal(&self) -> Result<Option<Goal>, String> {
        let value = self.client.send_request(
            "thread/goal/get",
            serde_json::json!({ "threadId": self.thread_id }),
        )?;
        Ok(parse_response::<GoalResponse>("thread/goal/get", value)?.goal)
    }

    pub fn set_goal(
        &self,
        objective: Option<&str>,
        status: Option<GoalStatus>,
        token_budget: Option<Option<u64>>,
    ) -> Result<Goal, String> {
        let mut params = serde_json::json!({ "threadId": self.thread_id });
        if let Some(objective) = objective {
            params["objective"] = serde_json::json!(objective);
        }
        if let Some(status) = status {
            params["status"] = serde_json::to_value(status).expect("goal status serializes");
        }
        if let Some(token_budget) = token_budget {
            params["tokenBudget"] = serde_json::json!(token_budget);
        }
        let value = self.client.send_request("thread/goal/set", params)?;
        parse_response::<GoalResponse>("thread/goal/set", value)?
            .goal
            .ok_or_else(|| "Invalid Codex thread/goal/set response: missing goal".to_string())
    }

    #[cfg(test)]
    fn clear_goal(&self) -> Result<bool, String> {
        let value = self.client.send_request(
            "thread/goal/clear",
            serde_json::json!({ "threadId": self.thread_id }),
        )?;
        Ok(parse_response::<GoalClearResponse>("thread/goal/clear", value)?.cleared)
    }

    pub fn send_turn(&self, content: &MessageContent) -> Result<(), String> {
        let input = crate::wire::codex_app_server::build_codex_input(content)?;
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

    pub fn interrupt(&self) -> Result<(), String> {
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

impl SessionControl for CodexControl {
    /// The first `turn/start`. Unlike a later turn it carries the output
    /// schema, and it leaves effort and service tier to the thread.
    fn open_turn(&mut self, content: &MessageContent) -> Result<(), String> {
        let opening = std::mem::take(&mut self.opening);
        let content = match opening.text {
            Some(text) => content.with_text(text),
            None => content.clone(),
        };
        let mut params = serde_json::json!({
            "threadId": self.thread_id.clone(),
            "cwd": self.cwd,
            "input": crate::wire::codex_app_server::build_codex_input(&content)?
        });
        if let Some(ref model) = self.model {
            params["model"] = serde_json::json!(model);
        }
        // Schema-constrained call (CAIRN-2505): the app-server maps
        // `outputSchema` to the provider's `final_output_json_schema`, so the
        // constrained result arrives as the turn's final agent message.
        if let Some(ref schema) = opening.output_schema {
            params["outputSchema"] = super::params::codex_strict_output_schema(schema);
        }
        let response = self.client.send_request("turn/start", params)?;
        if let Some(turn_id) = response
            .get("turn")
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str)
        {
            self.remember_turn_id(turn_id)?;
        }
        Ok(())
    }

    fn send(&mut self, input: &SessionInput<'_>) -> Result<(), String> {
        self.send_turn(input.content)
    }

    fn interrupt(&mut self) -> Result<(), String> {
        CodexControl::interrupt(self)
    }

    fn set_model(&mut self, _model: &str) -> Result<(), String> {
        Err("Changing Codex model mid-turn not yet supported".to_string())
    }

    fn set_permission_mode(&mut self, _mode: &str) -> Result<(), String> {
        Err("Changing Codex permission mode mid-session is not yet supported".to_string())
    }

    fn reply(&mut self, id: &Value, reply: ServerReply) -> Result<(), String> {
        self.client.reply(id, reply)
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
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
        CodexControl,
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
            CodexControl::new(
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
    fn warm_turn_sends_native_image_without_encoded_text() {
        let (stdin, requests, _) =
            scripted_stdin(None, vec![Ok(json!({ "turn": { "id": "turn-image" } }))]);
        let content = MessageContent {
            text: "inspect image".to_string(),
            images: vec![crate::wire::MessageImage {
                mime_type: "image/png".to_string(),
                bytes: b"warm-image-payload".to_vec(),
            }],
        };
        stdin.send_turn(&content).unwrap();

        let requests = requests.lock().unwrap();
        let input = requests[0].pointer("/params/input").unwrap();
        assert_eq!(input[1]["type"], "image");
        let url = input[1]["url"].as_str().unwrap();
        let encoded = url.split_once(',').unwrap().1;
        assert!(!input[0]["text"].as_str().unwrap().contains(encoded));
    }

    #[test]
    fn exact_interrupt_refuses_to_retarget_a_different_turn() {
        let (stdin, requests, _) = scripted_stdin(Some("turn-other"), vec![]);
        let error = stdin.interrupt_exact_and_wait("turn-leased").unwrap_err();
        assert!(error.contains("expected turn-leased, found turn-other"));
        assert!(requests.lock().unwrap().is_empty());
    }

    #[test]
    fn exact_interrupt_waits_for_the_leased_turn_to_clear() {
        let (stdin, requests, current_turn_id) =
            scripted_stdin(Some("turn-leased"), vec![Ok(Value::Null)]);
        let clearing_turn = current_turn_id.clone();
        let clearer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            *clearing_turn.lock().unwrap() = None;
        });
        assert!(matches!(
            stdin.interrupt_exact_and_wait("turn-leased").unwrap(),
            ExactTurnInterrupt::Acknowledged { .. }
        ));
        clearer.join().unwrap();
        assert_eq!(request_turn_ids(&requests), vec!["turn-leased"]);
    }

    #[test]
    fn exact_interrupt_rejects_a_post_delivery_turn_replacement() {
        let (stdin, _, current_turn_id) =
            scripted_stdin(Some("turn-leased"), vec![Ok(Value::Null)]);
        let replacing_turn = current_turn_id.clone();
        let replacer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            *replacing_turn.lock().unwrap() = Some("turn-other".to_string());
        });
        let error = stdin.interrupt_exact_and_wait("turn-leased").unwrap_err();
        replacer.join().unwrap();
        assert!(error.contains("mismatch after interrupt"));
    }

    #[test]
    fn interrupting_untracked_goal_turn_does_not_replace_newer_cached_owner() {
        let (stdin, requests, current_turn_id) =
            scripted_stdin(Some("turn-legitimate"), vec![Ok(Value::Null)]);

        stdin
            .interrupt_untracked_turn("turn-premature")
            .expect("premature provider turn is interrupted");

        assert_eq!(
            current_turn_id.lock().unwrap().as_deref(),
            Some("turn-legitimate"),
            "the notification identity must not overwrite the activation owner's cached turn"
        );
        assert_eq!(
            request_turn_ids(&requests),
            vec!["turn-premature".to_string()]
        );
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

    #[test]
    fn goal_requests_preserve_omitted_and_null_budget_semantics() {
        let goal = json!({ "threadId": "thread-1", "objective": "work", "status": "active",
            "tokenBudget": null, "tokensUsed": 0, "timeUsedSeconds": 0,
            "createdAt": 1, "updatedAt": 1 });
        let (stdin, requests, _) = scripted_stdin(
            None,
            vec![
                Ok(json!({"result": {"goal": null}})),
                Ok(json!({"result": {"goal": goal}})),
                Ok(json!({"result": {"cleared": true}})),
            ],
        );
        assert!(stdin.get_goal().unwrap().is_none());
        stdin
            .set_goal(Some("work"), Some(GoalStatus::Active), Some(None))
            .unwrap();
        assert!(stdin.clear_goal().unwrap());
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0]["method"], "thread/goal/get");
        assert_eq!(requests[1]["params"]["tokenBudget"], Value::Null);
        assert_eq!(requests[2]["method"], "thread/goal/clear");
    }
}
