//! iMessage channel provider backed by the `imsg` command-line tool.
//!
//! Process execution is deliberately isolated behind [`IMessageExecutor`]. The
//! local implementation uses Cairn's canonical process service, while a fleet
//! adapter can implement the same narrow contract without duplicating provider
//! behavior or command construction.

use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;

use super::{
    render_text_floor, ChannelCapabilities, ChannelHealth, ChannelProvider, InboundEvent,
    OutboundAsk, OutboundMessage, SentIds,
};
use crate::fleet::service_placement::ServiceLease;
use crate::services::{ProcessSpawner, SpawnConfig};
use cairn_common::executor_protocol::{
    ResidentProcessEventKind, ResidentProcessStatus, ResidentProcessStream,
};

const HEALTH_INTERVAL: Duration = Duration::from_secs(60);
const PLACED_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const PLACED_WATCH_KEY: &str = "imsg-watch";
/// Receiving clients silently ignore edits after the fifth edit to one message.
const MAX_PROPAGATING_EDITS: u8 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollOption {
    pub id: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PollVote {
    pub participant: String,
    pub option_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub stdout: String,
    pub stderr: String,
}

/// The reusable placement seam for iMessage processes. Implementations must
/// preserve argument boundaries and yield watch output one JSON line at a time.
#[async_trait]
pub trait IMessageExecutor: Send + Sync {
    fn placement_health(&self) -> Option<ChannelHealth> {
        None
    }

    async fn shutdown(&self) {}

    async fn run(&self, args: Vec<String>) -> Result<CommandResult, String>;
    async fn watch(
        &self,
        args: Vec<String>,
        lines: mpsc::Sender<Result<String, String>>,
    ) -> Result<(), String>;
}

pub struct LocalProcessExecutor {
    process: Arc<dyn ProcessSpawner>,
}

impl LocalProcessExecutor {
    pub fn new(process: Arc<dyn ProcessSpawner>) -> Self {
        Self { process }
    }
}

#[async_trait]
impl IMessageExecutor for LocalProcessExecutor {
    async fn run(&self, args: Vec<String>) -> Result<CommandResult, String> {
        let process = self.process.clone();
        tokio::task::spawn_blocking(move || {
            let output = process.run(SpawnConfig::new("imsg").args(&args))?;
            if !output.success {
                return Err(command_error(&output.stderr));
            }
            Ok(CommandResult {
                stdout: output.stdout,
                stderr: output.stderr,
            })
        })
        .await
        .map_err(|error| format!("imsg task failed: {error}"))?
    }

    async fn watch(
        &self,
        args: Vec<String>,
        lines: mpsc::Sender<Result<String, String>>,
    ) -> Result<(), String> {
        let process = self.process.clone();
        tokio::task::spawn_blocking(move || {
            let mut child = process.spawn(SpawnConfig::new("imsg").args(&args))?;
            let stdout = child
                .take_stdout()
                .ok_or_else(|| "imsg watch did not expose stdout".to_string())?;
            for line in std::io::BufReader::new(stdout).lines() {
                let line = line.map_err(|error| format!("reading imsg watch: {error}"));
                if lines.blocking_send(line).is_err() {
                    let _ = child.kill();
                    return Ok(());
                }
            }
            match child.try_wait() {
                Ok(Some(status)) if status.success() => Ok(()),
                Ok(Some(status)) => Err(format!("imsg watch exited with {status}")),
                Ok(None) => Err("imsg watch closed stdout before exiting".into()),
                Err(error) => Err(format!("waiting for imsg watch: {error}")),
            }
        })
        .await
        .map_err(|error| format!("imsg watch task failed: {error}"))?
    }
}

fn watch_args(since_rowid: i64) -> Vec<String> {
    vec![
        "watch".into(),
        "--json".into(),
        "--since-rowid".into(),
        since_rowid.to_string(),
    ]
}

fn command_error(stderr: &str) -> String {
    let detail = stderr.trim();
    if detail.is_empty() {
        "imsg exited unsuccessfully".into()
    } else {
        format!("imsg exited unsuccessfully: {detail}")
    }
}

pub struct PlacedProcessExecutor {
    lease: Arc<ServiceLease>,
}

impl PlacedProcessExecutor {
    pub(crate) fn new(lease: Arc<ServiceLease>) -> Self {
        Self { lease }
    }
}

#[async_trait]
impl IMessageExecutor for PlacedProcessExecutor {
    async fn shutdown(&self) {
        let _ = self.lease.stop_resident(PLACED_WATCH_KEY).await;
        let _ = self.lease.release().await;
    }

    fn placement_health(&self) -> Option<ChannelHealth> {
        use crate::fleet::service_placement::ServicePlacementHealth;

        match self.lease.health() {
            ServicePlacementHealth::Ready => None,
            ServicePlacementHealth::ExecutorOffline => Some(ChannelHealth::Unavailable {
                reason: format!("executor {} offline", self.lease.executor_name()),
            }),
            ServicePlacementHealth::ProcessDown {
                process_key,
                exit_code,
            } => Some(ChannelHealth::Unavailable {
                reason: format!("service process `{process_key}` exited with status {exit_code:?}"),
            }),
        }
    }

    async fn run(&self, args: Vec<String>) -> Result<CommandResult, String> {
        let output = self
            .lease
            .run_one_shot("imsg", args, PLACED_COMMAND_TIMEOUT)
            .await
            .map_err(|error| error.to_string())?;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if output.exit_code != Some(0) {
            return Err(command_error(&stderr));
        }
        Ok(CommandResult { stdout, stderr })
    }

    async fn watch(
        &self,
        args: Vec<String>,
        lines: mpsc::Sender<Result<String, String>>,
    ) -> Result<(), String> {
        // A deterministic stop/start converges after a runner restart and ensures
        // the new durable cursor is the only live watch specification.
        let _ = self.lease.stop_resident(PLACED_WATCH_KEY).await;
        let mut subscription = self
            .lease
            .start_resident(PLACED_WATCH_KEY, "imsg", args)
            .await
            .map_err(|error| error.to_string())?;
        let mut stdout = Vec::new();
        while let Some(event) = subscription.recv().await {
            match event.event {
                ResidentProcessEventKind::Output { stream, data, .. } => match stream {
                    ResidentProcessStream::Stdout | ResidentProcessStream::Pty => {
                        stdout.extend(data);
                        while let Some(newline) = stdout.iter().position(|byte| *byte == b'\n') {
                            let mut line = stdout.drain(..=newline).collect::<Vec<_>>();
                            line.pop();
                            if line.last() == Some(&b'\r') {
                                line.pop();
                            }
                            if lines
                                .send(Ok(String::from_utf8_lossy(&line).into_owned()))
                                .await
                                .is_err()
                            {
                                let _ = self.lease.stop_resident(PLACED_WATCH_KEY).await;
                                return Ok(());
                            }
                        }
                    }
                    ResidentProcessStream::Stderr => {
                        log::warn!(
                            "imsg watch on executor `{}`: {}",
                            self.lease.executor_name(),
                            String::from_utf8_lossy(&data).trim_end()
                        );
                    }
                },
                ResidentProcessEventKind::State {
                    status:
                        ResidentProcessStatus::Exited {
                            exit_code,
                            executor_lost,
                            ..
                        },
                } => {
                    if !executor_lost {
                        self.lease.mark_process_down(PLACED_WATCH_KEY, exit_code);
                    }
                    return Err(if executor_lost {
                        format!("executor {} offline", self.lease.executor_name())
                    } else {
                        format!("imsg watch exited with status {exit_code:?}")
                    });
                }
                ResidentProcessEventKind::State { .. } => {}
            }
        }
        Err("imsg watch event stream closed".into())
    }
}

#[derive(Default)]
struct WatchState {
    seen_votes: HashSet<PollVote>,
    options: HashMap<String, String>,
}

pub struct IMessageProvider {
    executor: Arc<dyn IMessageExecutor>,
    allow_from: Vec<String>,
    health: Arc<RwLock<ChannelHealth>>,
    inbound_rx: Mutex<Option<mpsc::Receiver<InboundEvent>>>,
    inbound_tx: mpsc::Sender<InboundEvent>,
    watch_state: Arc<Mutex<WatchState>>,
    edit_counts: Mutex<HashMap<String, u8>>,
}

impl IMessageProvider {
    pub fn new(executor: Arc<dyn IMessageExecutor>, allow_from: Vec<String>) -> Self {
        let (inbound_tx, inbound_rx) = mpsc::channel(128);
        Self {
            executor,
            allow_from,
            health: Arc::new(RwLock::new(ChannelHealth::Unavailable {
                reason: "health check has not completed".into(),
            })),
            inbound_rx: Mutex::new(Some(inbound_rx)),
            inbound_tx,
            watch_state: Arc::new(Mutex::new(WatchState::default())),
            edit_counts: Mutex::new(HashMap::new()),
        }
    }

    pub async fn refresh_health(&self) -> ChannelHealth {
        if let Some(health) = self.executor.placement_health() {
            *self.health.write().expect("iMessage health lock poisoned") = health.clone();
            return health;
        }
        let health = match self
            .executor
            .run(vec!["status".into(), "--json".into()])
            .await
        {
            Ok(output) => parse_status(&output.stdout),
            Err(error) => ChannelHealth::Unavailable { reason: error },
        };
        *self.health.write().expect("iMessage health lock poisoned") = health.clone();
        health
    }

    pub fn spawn_health_monitor(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let provider = self.clone();
        tokio::spawn(async move {
            loop {
                provider.refresh_health().await;
                tokio::time::sleep(HEALTH_INTERVAL).await;
            }
        })
    }

    /// Starts a watch at the durable row cursor. The caller owns restart policy
    /// and persists the rowid returned by [`parse_watch_line`] after processing.
    pub fn spawn_watch(
        self: &Arc<Self>,
        since_rowid: i64,
        cursors: mpsc::Sender<i64>,
    ) -> tokio::task::JoinHandle<Result<(), String>> {
        let provider = self.clone();
        tokio::spawn(async move {
            let (line_tx, mut line_rx) = mpsc::channel(128);
            let executor = provider.executor.clone();
            let watch =
                tokio::spawn(async move { executor.watch(watch_args(since_rowid), line_tx).await });
            while let Some(line) = line_rx.recv().await {
                let line = line?;
                let rowid = serde_json::from_str::<Value>(&line)
                    .ok()
                    .and_then(|value| i64_at(&value, &["rowid", "row_id", "id"]));
                match provider.ingest_watch_line(&line) {
                    Ok(Some((_rowid, event))) => {
                        if provider.inbound_tx.send(event).await.is_err() {
                            return Ok(());
                        }
                    }
                    Ok(None) => {}
                    Err(error) => log::warn!("ignoring malformed imsg watch event: {error}"),
                }
                if let Some(rowid) = rowid {
                    if cursors.send(rowid).await.is_err() {
                        return Ok(());
                    }
                }
            }
            watch
                .await
                .map_err(|error| format!("imsg watch join failed: {error}"))?
        })
    }

    pub fn ingest_watch_line(&self, line: &str) -> Result<Option<(i64, InboundEvent)>, String> {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| format!("invalid imsg watch JSON: {error}"))?;
        let sender = string_at(&value, &["sender", "handle", "participant"]).unwrap_or_default();
        if bool_at(&value, &["is_from_me", "isFromMe"]).unwrap_or(false)
            || !is_allowlisted(&sender, &self.allow_from)
        {
            return Ok(None);
        }
        let rowid = i64_at(&value, &["rowid", "row_id", "id"])
            .ok_or_else(|| "watch event missing rowid".to_string())?;

        if let Some(poll) = value.get("poll") {
            let original_guid = string_at(poll, &["original_guid", "originalGuid", "guid"])
                .or_else(|| string_at(&value, &["thread_originator_guid", "threadOriginatorGuid"]));
            let options = poll
                .get("options")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            Some(PollOption {
                                id: string_at(item, &["id", "option_id", "optionId"])?,
                                text: string_at(item, &["text", "label"])?,
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let mut state = self
                .watch_state
                .lock()
                .expect("iMessage watch lock poisoned");
            merge_poll_options(&mut state.options, &options);
            if let Some(votes) = poll.get("votes").and_then(Value::as_array) {
                let votes = votes.iter().filter_map(|vote| {
                    Some(PollVote {
                        participant: string_at(vote, &["participant", "sender", "handle"])?,
                        option_id: string_at(vote, &["option_id", "optionId", "id"])?,
                    })
                });
                let new = dedupe_cumulative_votes(&mut state.seen_votes, votes);
                if let (Some(vote), Some(bound_guid)) = (new.into_iter().next(), original_guid) {
                    if !is_allowlisted(&vote.participant, &self.allow_from) {
                        return Ok(None);
                    }
                    let option_text = state
                        .options
                        .get(&vote.option_id)
                        .cloned()
                        .unwrap_or_else(|| vote.option_id.clone());
                    return Ok(Some((
                        rowid,
                        InboundEvent::Selection {
                            bound_guid,
                            sender: vote.participant,
                            option_id: vote.option_id,
                            option_text,
                        },
                    )));
                }
            }
            // Poll creation and option-set mutation events are state updates, not asks.
            return Ok(None);
        }

        let text = string_at(&value, &["text", "body"]).unwrap_or_default();
        if text.is_empty() {
            return Ok(None);
        }
        let bound_guid = string_at(
            &value,
            &[
                "thread_originator_guid",
                "threadOriginatorGuid",
                "reply_to_guid",
            ],
        );
        Ok(Some((
            rowid,
            match bound_guid {
                Some(bound_guid) => InboundEvent::Reply {
                    bound_guid,
                    sender,
                    text,
                },
                None => InboundEvent::Bare { sender, text },
            },
        )))
    }

    pub async fn acknowledge(&self, conversation: &str) -> Result<(), String> {
        self.send_text(
            conversation,
            "No active ask — your message is visible in Cairn.",
        )
        .await
        .map(|_| ())
    }

    pub async fn edit(&self, guid: &str, text: &str) -> Result<(), String> {
        {
            let mut counts = self
                .edit_counts
                .lock()
                .expect("iMessage edit count lock poisoned");
            let count = counts.entry(guid.to_string()).or_default();
            if *count >= MAX_PROPAGATING_EDITS {
                return Err(format!(
                    "iMessage edit budget exhausted for {guid}; send a fresh status message"
                ));
            }
            *count += 1;
        }

        let result = self
            .executor
            .run(vec![
                "edit".into(),
                "--guid".into(),
                guid.into(),
                "--text".into(),
                text.into(),
            ])
            .await
            .map(|_| ());
        if result.is_err() {
            let mut counts = self
                .edit_counts
                .lock()
                .expect("iMessage edit count lock poisoned");
            if let Some(count) = counts.get_mut(guid) {
                *count = count.saturating_sub(1);
            }
        }
        result
    }

    async fn send_text(&self, conversation: &str, text: &str) -> Result<SentIds, String> {
        let output = self
            .executor
            .run(vec![
                "send".into(),
                "--to".into(),
                conversation.into(),
                "--text".into(),
                text.into(),
            ])
            .await?;
        self.sent_ids_or_history(conversation, text, &output.stdout)
            .await
    }

    async fn sent_ids_or_history(
        &self,
        conversation: &str,
        rendered: &str,
        send_stdout: &str,
    ) -> Result<SentIds, String> {
        if let Some(ids) = parse_sent_ids(send_stdout) {
            return Ok(ids);
        }
        let history = self
            .executor
            .run(vec![
                "history".into(),
                "--json".into(),
                "--chat".into(),
                conversation.into(),
                "--limit".into(),
                "20".into(),
            ])
            .await?;
        reconcile_history(&history.stdout, rendered).ok_or_else(|| {
            "imsg send succeeded but its message guid was not found in history".into()
        })
    }
}

#[async_trait]
impl ChannelProvider for IMessageProvider {
    fn capabilities(&self) -> ChannelCapabilities {
        let ready = matches!(self.health(), ChannelHealth::Ready);
        ChannelCapabilities {
            structured_asks: ready,
            open_options: ready,
            edit_in_place: ready,
            max_text_len: None,
        }
    }

    async fn send(&self, message: &OutboundMessage) -> Result<SentIds, String> {
        let body = render_text_floor(&message.ask);
        let rendered = if message.context_header.trim().is_empty() {
            body
        } else {
            format!("{}\n\n{body}", message.context_header)
        };
        if self.capabilities().structured_asks {
            if let OutboundAsk::Question { text, options, .. } = &message.ask {
                if !options.is_empty() {
                    let mut args = vec![
                        "poll".into(),
                        "send".into(),
                        "--chat".into(),
                        message.conversation.clone(),
                        "--question".into(),
                        format!("{}\n\n{text}", message.context_header),
                    ];
                    for option in options {
                        args.extend(["--option".into(), option.label.clone()]);
                    }
                    let output = self.executor.run(args).await?;
                    return self
                        .sent_ids_or_history(&message.conversation, text, &output.stdout)
                        .await;
                }
            }
        }
        self.send_text(&message.conversation, &rendered).await
    }

    fn subscribe(&self) -> mpsc::Receiver<InboundEvent> {
        self.inbound_rx
            .lock()
            .expect("iMessage subscription lock poisoned")
            .take()
            .expect("iMessage provider supports one inbound subscriber")
    }

    fn health(&self) -> ChannelHealth {
        self.health
            .read()
            .expect("iMessage health lock poisoned")
            .clone()
    }
}

fn parse_status(stdout: &str) -> ChannelHealth {
    let Ok(value) = serde_json::from_str::<Value>(stdout) else {
        return ChannelHealth::Unavailable {
            reason: "imsg status returned invalid JSON".into(),
        };
    };
    let bridge = bool_at(&value, &["bridge", "bridge_ready", "bridgeReady", "polls"])
        .or_else(|| {
            value
                .get("status")
                .and_then(Value::as_str)
                .map(|s| matches!(s, "ready" | "ok" | "running"))
        })
        .unwrap_or(false);
    if bridge {
        ChannelHealth::Ready
    } else {
        let reason = string_at(&value, &["reason", "error", "message"])
            .unwrap_or_else(|| "IMCore bridge unavailable; plain text remains available".into());
        ChannelHealth::Degraded { reason }
    }
}

fn parse_sent_ids(stdout: &str) -> Option<SentIds> {
    let value: Value = serde_json::from_str(stdout).ok()?;
    let primary_guid = string_at(
        &value,
        &[
            "poll_guid",
            "pollGuid",
            "guid",
            "message_guid",
            "messageGuid",
        ],
    )?;
    let caption_guid = string_at(&value, &["caption_guid", "captionGuid"]);
    Some(SentIds {
        primary_guid,
        caption_guid,
    })
}

fn reconcile_history(stdout: &str, rendered: &str) -> Option<SentIds> {
    let value: Value = serde_json::from_str(stdout).ok()?;
    let rows = value
        .as_array()
        .or_else(|| value.get("messages")?.as_array())?;
    let mut primary = None;
    let mut caption = None;
    for row in rows.iter().rev() {
        if !bool_at(row, &["is_from_me", "isFromMe"]).unwrap_or(false) {
            continue;
        }
        let text = string_at(row, &["text", "body"]).unwrap_or_default();
        let guid = string_at(row, &["guid", "message_guid", "messageGuid"])?;
        if text == rendered || rendered.ends_with(&text) || text.ends_with(rendered) {
            if string_at(row, &["thread_originator_guid", "threadOriginatorGuid"]).is_some() {
                caption.get_or_insert(guid);
            } else {
                primary.get_or_insert(guid);
            }
        }
    }
    primary
        .or_else(|| caption.clone())
        .map(|primary_guid| SentIds {
            primary_guid,
            caption_guid: caption,
        })
}

fn string_at(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key)?.as_str().map(str::to_string))
}
fn bool_at(value: &Value, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| value.get(*key)?.as_bool())
}
fn i64_at(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| value.get(*key)?.as_i64())
}

/// Canonicalizes an iMessage handle for allowlist comparison.
pub fn normalize_handle(handle: &str) -> String {
    let trimmed = handle.trim();
    if trimmed.contains('@') {
        return trimmed.to_lowercase();
    }
    let has_plus = trimmed.starts_with('+');
    let digits: String = trimmed.chars().filter(char::is_ascii_digit).collect();
    if digits.is_empty() {
        trimmed.to_lowercase()
    } else if has_plus {
        format!("+{digits}")
    } else {
        digits
    }
}

pub fn is_allowlisted(sender: &str, allow_from: &[String]) -> bool {
    let sender = normalize_handle(sender);
    allow_from
        .iter()
        .any(|allowed| normalize_handle(allowed) == sender)
}

pub fn merge_poll_options(options: &mut HashMap<String, String>, mutation: &[PollOption]) {
    for option in mutation {
        options.insert(option.id.clone(), option.text.clone());
    }
}

pub fn dedupe_cumulative_votes(
    seen: &mut HashSet<PollVote>,
    cumulative: impl IntoIterator<Item = PollVote>,
) -> Vec<PollVote> {
    cumulative
        .into_iter()
        .filter(|vote| seen.insert(vote.clone()))
        .collect()
}

pub fn parse_reply_number(reply: &str, option_count: usize) -> Option<usize> {
    let digits: String = reply
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    let number = digits.parse::<usize>().ok()?;
    (1..=option_count).contains(&number).then_some(number - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct FakeExecutor {
        results: Mutex<VecDeque<Result<CommandResult, String>>>,
        calls: Mutex<Vec<Vec<String>>>,
        watch_lines: Vec<String>,
    }

    impl FakeExecutor {
        fn new(outputs: impl IntoIterator<Item = &'static str>) -> Self {
            Self {
                results: Mutex::new(
                    outputs
                        .into_iter()
                        .map(|stdout| {
                            Ok(CommandResult {
                                stdout: stdout.into(),
                                stderr: String::new(),
                            })
                        })
                        .collect(),
                ),
                calls: Mutex::new(Vec::new()),
                watch_lines: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl IMessageExecutor for FakeExecutor {
        async fn run(&self, args: Vec<String>) -> Result<CommandResult, String> {
            self.calls.lock().unwrap().push(args);
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected command")
        }
        async fn watch(
            &self,
            args: Vec<String>,
            lines: mpsc::Sender<Result<String, String>>,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push(args);
            for line in &self.watch_lines {
                lines.send(Ok(line.clone())).await.unwrap();
            }
            Ok(())
        }
    }

    fn message() -> OutboundMessage {
        OutboundMessage {
            intent_id: "intent".into(),
            conversation: "+15551234567".into(),
            context_header: "[CAIRN-3373 · builder]".into(),
            ask: OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 0,
                text: "Which path?".into(),
                options: vec![super::super::AskOption {
                    label: "New".into(),
                    description: None,
                }],
            },
        }
    }

    #[tokio::test]
    async fn watch_starts_at_durable_cursor_and_reports_progress() {
        let fake = Arc::new(FakeExecutor {
            results: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
            watch_lines: vec![r#"{"rowid":42,"sender":"+14155550123","text":"hello"}"#.into()],
        });
        let provider = Arc::new(IMessageProvider::new(
            fake.clone(),
            vec!["+14155550123".into()],
        ));
        let (cursor_tx, mut cursor_rx) = mpsc::channel(1);

        provider.spawn_watch(41, cursor_tx).await.unwrap().unwrap();

        assert_eq!(cursor_rx.recv().await, Some(42));
        assert_eq!(fake.calls.lock().unwrap()[0], watch_args(41));
    }

    #[tokio::test]
    async fn degraded_health_sends_text_and_reconciles_guid_from_history() {
        let fake = Arc::new(FakeExecutor::new([
            r#"{"status":"degraded","reason":"bridge down"}"#,
            "{}",
            r#"[{"guid":"m1","text":"[CAIRN-3373 · builder]\n\nWhich path?\n\n1. New\n\nReply to this message with a number or your answer.","is_from_me":true}]"#,
        ]));
        let provider = IMessageProvider::new(fake.clone(), vec!["+15551234567".into()]);
        assert!(matches!(
            provider.refresh_health().await,
            ChannelHealth::Degraded { .. }
        ));
        assert_eq!(provider.send(&message()).await.unwrap().primary_guid, "m1");
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[1][0], "send");
        assert_eq!(calls[2][0], "history");
    }

    #[tokio::test]
    async fn ready_health_sends_poll_with_argument_boundaries() {
        let fake = Arc::new(FakeExecutor::new([
            r#"{"bridge_ready":true}"#,
            r#"{"poll_guid":"poll-1","caption_guid":"caption-1"}"#,
        ]));
        let provider = IMessageProvider::new(fake.clone(), vec!["+15551234567".into()]);
        assert_eq!(provider.refresh_health().await, ChannelHealth::Ready);
        let ids = provider.send(&message()).await.unwrap();
        assert_eq!(ids.caption_guid.as_deref(), Some("caption-1"));
        let calls = fake.calls.lock().unwrap();
        assert_eq!(&calls[1][..2], &["poll", "send"]);
        assert!(calls[1].windows(2).any(|pair| pair == ["--option", "New"]));
    }

    #[tokio::test]
    async fn edit_budget_stops_before_clients_silently_drop_updates() {
        let fake = Arc::new(FakeExecutor::new(["{}", "{}", "{}", "{}", "{}"]));
        let provider = IMessageProvider::new(fake.clone(), vec![]);
        for version in 1..=MAX_PROPAGATING_EDITS {
            provider
                .edit("message-guid", &format!("v{version}"))
                .await
                .unwrap();
        }
        let error = provider.edit("message-guid", "v6").await.unwrap_err();
        assert!(error.contains("send a fresh status message"));
        assert_eq!(
            fake.calls.lock().unwrap().len(),
            MAX_PROPAGATING_EDITS as usize
        );
    }

    #[test]
    fn parses_allowlisted_reply_vote_mutations_and_echo_guard() {
        let provider =
            IMessageProvider::new(Arc::new(FakeExecutor::new([])), vec!["+14155550123".into()]);
        let mutation = r#"{"rowid":10,"sender":"+14155550123","poll":{"original_guid":"poll","options":[{"id":"a","text":"Alpha"}],"votes":[]}}"#;
        assert_eq!(provider.ingest_watch_line(mutation).unwrap(), None);
        let vote = r#"{"rowid":11,"sender":"+14155550123","poll":{"original_guid":"poll","options":[{"id":"a","text":"Alpha"}],"votes":[{"participant":"+14155550123","option_id":"a"}]}}"#;
        assert!(
            matches!(provider.ingest_watch_line(vote).unwrap(), Some((11, InboundEvent::Selection { option_text, .. })) if option_text == "Alpha")
        );
        assert_eq!(provider.ingest_watch_line(vote).unwrap(), None);
        let reply =
            r#"{"rowid":12,"sender":"+14155550123","text":"2","thread_originator_guid":"poll"}"#;
        assert!(matches!(
            provider.ingest_watch_line(reply).unwrap(),
            Some((12, InboundEvent::Reply { .. }))
        ));
        let echo = r#"{"rowid":13,"sender":"+14155550123","text":"mine","is_from_me":true}"#;
        assert_eq!(provider.ingest_watch_line(echo).unwrap(), None);
    }

    #[test]
    fn normalizes_handles_and_parses_reply_numbers() {
        let allowlist = vec![" +1 (415) 555-0123 ".into(), "USER@Example.COM".into()];
        assert!(is_allowlisted("+14155550123", &allowlist));
        assert!(is_allowlisted(" user@example.com ", &allowlist));
        assert!(!is_allowlisted("+14155559999", &allowlist));
        assert_eq!(parse_reply_number(" 2. New", 3), Some(1));
        assert_eq!(parse_reply_number("4", 3), None);
    }
}
