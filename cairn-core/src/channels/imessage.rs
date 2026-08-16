//! iMessage channel provider backed by the `imsg` command-line tool.
//!
//! Process execution is deliberately isolated behind [`IMessageExecutor`]. The
//! local implementation uses Cairn's canonical process service, while a fleet
//! adapter can implement the same narrow contract without duplicating provider
//! behavior or command construction.

use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::{
    render_text_floor, ChannelCapabilities, ChannelHealth, ChannelProvider, InboundEvent,
    OperatorPresence, OutboundAsk, OutboundMessage, PollSelectionChange, ResolvedQuestionMessage,
    SentIds,
};
use crate::fleet::service_placement::ServiceLease;
use crate::services::{ProcessSpawner, SpawnConfig};
use cairn_common::executor_protocol::{
    ResidentProcessEventKind, ResidentProcessStatus, ResidentProcessStream,
};

const HEALTH_INTERVAL: Duration = Duration::from_secs(60);
const PLACED_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const PLACED_WATCH_KEY: &str = "imsg-watch";
/// What the watch calls itself on a surface that shows running work. The key
/// above addresses it; this names it.
const PLACED_WATCH_ROLE: &str = "watch";
/// Receiving clients silently ignore edits after the fifth edit to one message.
const MAX_PROPAGATING_EDITS: u8 = 5;
const UNSEND_WINDOW: Duration = Duration::from_secs(2 * 60);
const EDIT_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Keep markdown as Cairn's authoring format while producing both clean channel
/// text and the native IMCore formatting ranges supported by `imsg send-rich`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
struct TextFormat {
    start: usize,
    length: usize,
    styles: Vec<&'static str>,
}

fn reports_rich_send(stdout: &str) -> bool {
    serde_json::from_str::<Value>(stdout)
        .ok()
        .and_then(|value| value.get("rpc_methods").cloned())
        .and_then(|methods| methods.as_array().cloned())
        .is_some_and(|methods| {
            methods
                .iter()
                .any(|method| method.as_str() == Some("send.rich"))
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenderedText {
    text: String,
    formats: Vec<TextFormat>,
}

impl RenderedText {
    fn append(&mut self, other: Self) {
        let offset = self.text.encode_utf16().count();
        self.text.push_str(&other.text);
        self.formats
            .extend(other.formats.into_iter().map(|mut format| {
                format.start += offset;
                format
            }));
    }
}

fn render_channel_text(markdown: &str) -> RenderedText {
    let mut rendered = RenderedText {
        text: String::new(),
        formats: Vec::new(),
    };
    let mut open_styles: Vec<(&'static str, usize)> = Vec::new();
    let mut link: Option<String> = None;
    // The open list stack, each entry carrying the next ordinal for an ordered
    // list and `None` for a bullet list. Numbering is load-bearing rather than
    // decorative: the text floor for an ask is a numbered list whose closing line
    // tells the operator to "reply with a number", and the reply parser maps that
    // number back to an option by position. Rendering every item as a bullet took
    // the numbers off the operator's screen while the parser still expected them.
    let mut list_ordinals: Vec<Option<u64>> = Vec::new();
    let options = Options::ENABLE_STRIKETHROUGH;

    for event in Parser::new_ext(markdown, options) {
        let position = rendered.text.encode_utf16().count();
        match event {
            Event::Start(Tag::Strong) => open_styles.push(("bold", position)),
            Event::Start(Tag::Emphasis) => open_styles.push(("italic", position)),
            Event::Start(Tag::Strikethrough) => open_styles.push(("strikethrough", position)),
            Event::Start(Tag::Heading { .. }) => open_styles.push(("bold", position)),
            Event::End(TagEnd::Strong) => close_style(&mut rendered, &mut open_styles, "bold"),
            Event::End(TagEnd::Emphasis) => close_style(&mut rendered, &mut open_styles, "italic"),
            Event::End(TagEnd::Strikethrough) => {
                close_style(&mut rendered, &mut open_styles, "strikethrough")
            }
            Event::End(TagEnd::Heading(_)) => {
                close_style(&mut rendered, &mut open_styles, "bold");
                ensure_newlines(&mut rendered.text, 2);
            }
            Event::Start(Tag::Link { dest_url, .. })
            | Event::Start(Tag::Image { dest_url, .. }) => {
                link = Some(dest_url.into_string());
            }
            Event::End(TagEnd::Link) | Event::End(TagEnd::Image) => {
                if let Some(destination) = link.take() {
                    rendered.text.push_str(&destination);
                }
            }
            Event::Start(Tag::List(first_ordinal)) => {
                // A list always opens on its own line, including one nested
                // inside an item, whose text would otherwise run into it.
                if !rendered.text.is_empty() {
                    ensure_newlines(&mut rendered.text, 1);
                }
                list_ordinals.push(first_ordinal);
            }
            Event::Start(Tag::Item) => {
                let indent = "  ".repeat(list_ordinals.len().saturating_sub(1));
                rendered.text.push_str(&indent);
                match list_ordinals.last_mut() {
                    Some(Some(ordinal)) => {
                        rendered.text.push_str(&format!("{ordinal}. "));
                        *ordinal += 1;
                    }
                    _ => rendered.text.push_str("- "),
                }
            }
            Event::End(TagEnd::Item) => ensure_newlines(&mut rendered.text, 1),
            Event::End(TagEnd::List(_)) => {
                list_ordinals.pop();
                // A nested list ends inside its parent item, so it takes a line
                // break; only the outermost list closes with a blank line.
                ensure_newlines(
                    &mut rendered.text,
                    if list_ordinals.is_empty() { 2 } else { 1 },
                );
            }
            Event::End(TagEnd::Paragraph) | Event::End(TagEnd::CodeBlock) => {
                ensure_newlines(&mut rendered.text, 2)
            }
            Event::Text(text) | Event::Code(text) if link.is_none() => {
                rendered.text.push_str(&text)
            }
            Event::SoftBreak | Event::HardBreak => ensure_newlines(&mut rendered.text, 1),
            Event::Rule => rendered.text.push_str("---\n"),
            _ => {}
        }
    }
    rendered.text = rendered.text.trim().to_string();
    rendered
        .formats
        .retain(|format| format.start + format.length <= rendered.text.encode_utf16().count());
    rendered
}

fn close_style(
    rendered: &mut RenderedText,
    open: &mut Vec<(&'static str, usize)>,
    style: &'static str,
) {
    if let Some(index) = open.iter().rposition(|(candidate, _)| *candidate == style) {
        let (_, start) = open.remove(index);
        rendered.formats.push(TextFormat {
            start,
            length: rendered.text.encode_utf16().count() - start,
            styles: vec![style],
        });
    }
}

fn ensure_newlines(text: &mut String, count: usize) {
    let present = text.chars().rev().take_while(|ch| *ch == '\n').count();
    for _ in present..count {
        text.push('\n');
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollOption {
    pub id: String,
    pub text: String,
}

fn sender_mismatch_diagnostic(sender: &str, allow_from: &[String]) -> String {
    let sender = if sender.trim().is_empty() {
        "<missing>"
    } else {
        sender
    };
    let configured = if allow_from.is_empty() {
        "<empty>".to_string()
    } else {
        allow_from.join(", ")
    };
    format!(
        "iMessage inbound sender `{sender}` does not match configured operator handle(s) `{configured}`"
    )
}

fn log_sender_mismatch(sender: &str, allow_from: &[String]) {
    log::warn!("{}", sender_mismatch_diagnostic(sender, allow_from));
}

fn nonzero(value: i64) -> Option<i64> {
    (value != 0).then_some(value)
}

const ACTIVE_INPUT_IDLE_SECONDS: u64 = 60;
const OPERATOR_PRESENCE_SCRIPT: &str = r#"
idle=$(/usr/sbin/ioreg -c IOHIDSystem -d 4 | /usr/bin/awk '/HIDIdleTime/ { print int($NF / 1000000000); exit }')
locked=$(/usr/sbin/ioreg -n Root -d 1 | /usr/bin/awk '/"IOConsoleLocked"/ { print ($NF == "Yes") ? 1 : 0; exit }')
[ -n "$idle" ] && [ -n "$locked" ] || exit 1
/usr/bin/printf '%s %s\n' "$idle" "$locked"
"#;

async fn probe_operator_presence_local(
    process: Arc<dyn ProcessSpawner>,
) -> Result<OperatorPresence, String> {
    tokio::task::spawn_blocking(move || {
        let output =
            process.run(SpawnConfig::new("/bin/sh").args(["-c", OPERATOR_PRESENCE_SCRIPT]))?;
        if !output.success {
            return Err(output.stderr);
        }
        parse_operator_presence(&output.stdout)
    })
    .await
    .map_err(|error| format!("operator presence task failed: {error}"))?
}

fn parse_operator_presence(stdout: &str) -> Result<OperatorPresence, String> {
    let mut fields = stdout.split_whitespace();
    let idle_seconds = fields
        .next()
        .ok_or_else(|| "operator presence omitted input idle time".to_string())?
        .parse::<u64>()
        .map_err(|error| format!("invalid input idle time: {error}"))?;
    let locked = match fields.next() {
        Some("0") => false,
        Some("1") => true,
        _ => return Err("operator presence omitted screen lock state".into()),
    };
    Ok(if !locked && idle_seconds < ACTIVE_INPUT_IDLE_SECONDS {
        OperatorPresence::Present
    } else {
        OperatorPresence::Away
    })
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

    async fn operator_presence(&self) -> Result<OperatorPresence, String> {
        Ok(OperatorPresence::Away)
    }

    async fn run(&self, args: Vec<String>) -> Result<CommandResult, String>;
    async fn watch(
        &self,
        args: Vec<String>,
        lines: mpsc::Sender<Result<String, String>>,
        started: oneshot::Sender<()>,
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
    async fn operator_presence(&self) -> Result<OperatorPresence, String> {
        probe_operator_presence_local(self.process.clone()).await
    }
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
        started: oneshot::Sender<()>,
    ) -> Result<(), String> {
        let process = self.process.clone();
        tokio::task::spawn_blocking(move || {
            let mut child = process.spawn(SpawnConfig::new("imsg").args(&args))?;
            let stdout = child
                .take_stdout()
                .ok_or_else(|| "imsg watch did not expose stdout".to_string())?;
            let _ = started.send(());
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

fn placed_presence_fallback() -> OperatorPresence {
    OperatorPresence::Away
}

#[async_trait]
impl IMessageExecutor for PlacedProcessExecutor {
    async fn operator_presence(&self) -> Result<OperatorPresence, String> {
        Ok(placed_presence_fallback())
    }

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
        started: oneshot::Sender<()>,
    ) -> Result<(), String> {
        // A deterministic stop/start converges after a runner restart and ensures
        // the new durable cursor is the only live watch specification.
        let _ = self.lease.stop_resident(PLACED_WATCH_KEY).await;
        let mut subscription = self
            .lease
            .start_resident(PLACED_WATCH_KEY, PLACED_WATCH_ROLE, "imsg", args)
            .await
            .map_err(|error| error.to_string())?;
        let _ = started.send(());
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
    votes_by_poll: HashMap<String, HashSet<PollVote>>,
}

pub struct IMessageProvider {
    executor: Arc<dyn IMessageExecutor>,
    allow_from: Vec<String>,
    health: Arc<RwLock<ChannelHealth>>,
    inbound_rx: Mutex<Option<mpsc::Receiver<InboundEvent>>>,
    inbound_tx: mpsc::Sender<InboundEvent>,
    watch_state: Arc<Mutex<WatchState>>,
    watch_active: AtomicBool,
    watch_started_at: AtomicI64,
    last_receipt_at: AtomicI64,
    last_receipt_rowid: AtomicI64,
    edit_counts: Mutex<HashMap<String, u8>>,
    rich_text_ready: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchLiveness {
    pub active: bool,
    pub started_at: Option<i64>,
    pub last_receipt_at: Option<i64>,
    pub last_receipt_rowid: Option<i64>,
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
            watch_active: AtomicBool::new(false),
            watch_started_at: AtomicI64::new(0),
            last_receipt_at: AtomicI64::new(0),
            last_receipt_rowid: AtomicI64::new(0),
            edit_counts: Mutex::new(HashMap::new()),
            rich_text_ready: AtomicBool::new(false),
        }
    }

    pub fn watch_liveness(&self) -> WatchLiveness {
        WatchLiveness {
            active: self.watch_active.load(Ordering::Acquire),
            started_at: nonzero(self.watch_started_at.load(Ordering::Acquire)),
            last_receipt_at: nonzero(self.last_receipt_at.load(Ordering::Acquire)),
            last_receipt_rowid: nonzero(self.last_receipt_rowid.load(Ordering::Acquire)),
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
            Ok(output) => {
                self.rich_text_ready
                    .store(reports_rich_send(&output.stdout), Ordering::Release);
                parse_status(&output.stdout)
            }
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
            struct ActiveWatch<'a>(&'a AtomicBool);
            impl Drop for ActiveWatch<'_> {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            let _active_watch = ActiveWatch(&provider.watch_active);
            let (line_tx, mut line_rx) = mpsc::channel(128);
            let (started_tx, started_rx) = oneshot::channel();
            let executor = provider.executor.clone();
            let watch = tokio::spawn(async move {
                executor
                    .watch(watch_args(since_rowid), line_tx, started_tx)
                    .await
            });
            if started_rx.await.is_ok() {
                provider.watch_active.store(true, Ordering::Release);
                provider
                    .watch_started_at
                    .store(chrono::Utc::now().timestamp(), Ordering::Release);
            }
            while let Some(line) = line_rx.recv().await {
                let line = line?;
                let rowid = serde_json::from_str::<Value>(&line)
                    .ok()
                    .and_then(|value| i64_at(&value, &["rowid", "row_id", "id"]));
                provider
                    .last_receipt_at
                    .store(chrono::Utc::now().timestamp(), Ordering::Release);
                if let Some(rowid) = rowid {
                    provider.last_receipt_rowid.store(rowid, Ordering::Release);
                }
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
        if bool_at(&value, &["is_from_me", "isFromMe"]).unwrap_or(false) {
            return Ok(None);
        }
        if !is_allowlisted(&sender, &self.allow_from) {
            log_sender_mismatch(&sender, &self.allow_from);
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
                // A vote event carries no option set of its own - it names the chosen
                // option inline. Cairn never sees the creation event that would have
                // populated the option map (its own poll is from_me), so the label on
                // the vote is the only thing standing between an answer and a raw UUID.
                let labelled = votes
                    .iter()
                    .filter_map(|vote| {
                        Some(PollOption {
                            id: string_at(vote, &["option_id", "optionId", "id"])?,
                            text: string_at(vote, &["option_text", "optionText"])?,
                        })
                    })
                    .collect::<Vec<_>>();
                merge_poll_options(&mut state.options, &labelled);
                let votes = votes.iter().filter_map(|vote| {
                    Some(PollVote {
                        participant: string_at(vote, &["participant", "sender", "handle"])?,
                        option_id: string_at(vote, &["option_id", "optionId", "id"])?,
                    })
                });
                let current = votes.collect::<HashSet<_>>();
                let changes = original_guid.as_ref().map(|guid| {
                    let previous = state.votes_by_poll.entry(guid.clone()).or_default();
                    let mut changes = current
                        .difference(previous)
                        .cloned()
                        .map(|vote| (vote, true))
                        .collect::<Vec<_>>();
                    changes.extend(
                        previous
                            .difference(&current)
                            .cloned()
                            .map(|vote| (vote, false)),
                    );
                    *previous = current.clone();
                    changes
                });
                state.seen_votes.extend(current);
                if let (Some(changes), Some(bound_guid)) = (changes, original_guid) {
                    let Some(participant) =
                        changes.first().map(|(vote, _)| vote.participant.clone())
                    else {
                        return Ok(None);
                    };
                    if !is_allowlisted(&participant, &self.allow_from) {
                        log_sender_mismatch(&participant, &self.allow_from);
                        return Ok(None);
                    }
                    return Ok(Some((
                        rowid,
                        InboundEvent::Selections {
                            conversation: format!("imessage:{participant}"),
                            bound_guid,
                            sender: participant,
                            changes: changes
                                .into_iter()
                                .map(|(vote, selected)| PollSelectionChange {
                                    option_text: state
                                        .options
                                        .get(&vote.option_id)
                                        .cloned()
                                        .unwrap_or_else(|| vote.option_id.clone()),
                                    option_id: vote.option_id,
                                    selected,
                                })
                                .collect(),
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
                    conversation: format!("imessage:{sender}"),
                    bound_guid,
                    sender,
                    text,
                },
                None => InboundEvent::Bare {
                    conversation: format!("imessage:{sender}"),
                    sender,
                    text,
                },
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

    /// Edits a message in place. `imsg edit` addresses both the chat and the message,
    /// so the conversation travels with the guid; a chat imsg cannot resolve has no
    /// message to edit.
    pub async fn edit(&self, conversation: &str, guid: &str, text: &str) -> Result<(), String> {
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

        let result = match self.resolve_chat(conversation).await {
            Some(chat) => self
                .executor
                .run(vec![
                    "edit".into(),
                    "--chat".into(),
                    chat.guid,
                    "--message".into(),
                    guid.into(),
                    "--new-text".into(),
                    text.into(),
                ])
                .await
                .map(|_| ()),
            None => Err(format!("imsg reports no chat for {conversation}")),
        };
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

    /// Only `imsg send` addresses a conversation by bare handle; `poll send` wants a
    /// chat guid and `history` a chat rowid. The configured recipient is a handle, so
    /// both are resolved against the chats imsg reports rather than synthesized. A
    /// handle with no chat yet resolves to nothing, which keeps the ask on the text
    /// floor - and that send creates the chat, making the next ask pollable.
    async fn poll_sent_ids(
        &self,
        conversation: &str,
        caption: &str,
        send_stdout: &str,
    ) -> Result<SentIds, String> {
        let mut ids = parse_sent_ids(send_stdout)
            .ok_or_else(|| "imsg poll send returned no message guid".to_string())?;
        // The poll is already on the wire. Caption reconciliation enriches its
        // binding for replies and cleanup, but must never turn that successful
        // send into a retry that would duplicate the poll.
        if let Some(chat) = self.resolve_chat(conversation).await {
            if let Ok(history) = self
                .executor
                .run(vec![
                    "history".into(),
                    "--json".into(),
                    "--chat-id".into(),
                    chat.id.to_string(),
                    "--limit".into(),
                    "20".into(),
                ])
                .await
            {
                ids.caption_guid =
                    reconcile_history(&history.stdout, caption).map(|caption| caption.primary_guid);
            }
        }
        Ok(ids)
    }

    async fn cleanup_guid(
        &self,
        chat_guid: &str,
        guid: &str,
        age: Duration,
        receipt: &str,
    ) -> Result<(), String> {
        if age <= UNSEND_WINDOW
            && self
                .executor
                .run(vec![
                    "unsend".into(),
                    "--chat".into(),
                    chat_guid.into(),
                    "--message".into(),
                    guid.into(),
                ])
                .await
                .is_ok()
        {
            return Ok(());
        }
        if age <= EDIT_WINDOW {
            return self
                .executor
                .run(vec![
                    "edit".into(),
                    "--chat".into(),
                    chat_guid.into(),
                    "--message".into(),
                    guid.into(),
                    "--new-text".into(),
                    receipt.into(),
                ])
                .await
                .map(|_| ());
        }
        Ok(())
    }

    async fn cleanup_question(&self, message: &ResolvedQuestionMessage) -> Result<(), String> {
        let Some(chat) = self.resolve_chat(&message.conversation).await else {
            return Err(format!("imsg reports no chat for {}", message.conversation));
        };
        let age_seconds = chrono::Utc::now()
            .timestamp()
            .saturating_sub(message.sent_at) as u64;
        let age = Duration::from_secs(age_seconds);
        let mut failures = Vec::new();
        for guid in std::iter::once(&message.provider_guid).chain(message.caption_guid.iter()) {
            if let Err(error) = self
                .cleanup_guid(&chat.guid, guid, age, &message.receipt)
                .await
            {
                failures.push(format!("{guid}: {error}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    async fn resolve_chat(&self, conversation: &str) -> Option<Chat> {
        let output = self
            .executor
            .run(vec![
                "chats".into(),
                "--json".into(),
                "--limit".into(),
                "50".into(),
            ])
            .await
            .ok()?;
        find_chat(&output.stdout, conversation)
    }

    async fn send_text(&self, conversation: &str, text: &str) -> Result<SentIds, String> {
        let output = self
            .executor
            .run(vec![
                "send".into(),
                "--json".into(),
                "--to".into(),
                conversation.into(),
                "--text".into(),
                text.into(),
            ])
            .await?;
        self.sent_ids_or_history(conversation, text, &output.stdout)
            .await
    }

    async fn send_rendered(
        &self,
        conversation: &str,
        rendered: &RenderedText,
    ) -> Result<SentIds, String> {
        if !rendered.formats.is_empty() && self.rich_text_ready.load(Ordering::Acquire) {
            if let Some(chat) = self.resolve_chat(conversation).await {
                let output = self
                    .executor
                    .run(vec![
                        "send-rich".into(),
                        "--json".into(),
                        "--chat".into(),
                        chat.guid,
                        "--text".into(),
                        rendered.text.clone(),
                        "--format".into(),
                        serde_json::to_string(&rendered.formats)
                            .map_err(|error| error.to_string())?,
                    ])
                    .await?;
                return self
                    .sent_ids_or_history(conversation, &rendered.text, &output.stdout)
                    .await;
            }
        }
        self.send_text(conversation, &rendered.text).await
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
        let chat = self
            .resolve_chat(conversation)
            .await
            .ok_or_else(|| format!("imsg reports no chat for {conversation}"))?;
        let history = self
            .executor
            .run(vec![
                "history".into(),
                "--json".into(),
                "--chat-id".into(),
                chat.id.to_string(),
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
        let body = render_channel_text(&render_text_floor(&message.ask));
        let context_header = render_channel_text(&message.context_header);
        let mut rendered = if message.context_header.trim().is_empty() {
            body.clone()
        } else {
            context_header.clone()
        };
        if !message.context_header.trim().is_empty() {
            rendered.append(RenderedText {
                text: "\n\n".into(),
                formats: Vec::new(),
            });
            rendered.append(body);
        }
        if self.capabilities().structured_asks {
            let poll = match &message.ask {
                OutboundAsk::Question { text, options, .. } => Some((
                    text.as_str(),
                    options
                        .iter()
                        .map(|option| option.label.as_str())
                        .collect::<Vec<_>>(),
                )),
                OutboundAsk::Permission { summary, .. } => {
                    Some((summary.as_str(), vec!["Approve", "Deny"]))
                }
                OutboundAsk::Notify { .. } => None,
            };
            if let Some((text, options)) = poll {
                // A Messages poll balloon needs at least two options; imsg rejects a
                // single `--option`. A one-option ask takes the numbered-text floor
                // rather than failing the delivery outright.
                if options.len() >= 2 {
                    if let Some(chat) = self.resolve_chat(&message.conversation).await {
                        let mut args = vec![
                            "poll".into(),
                            "send".into(),
                            "--json".into(),
                            "--chat".into(),
                            chat.guid,
                            "--question".into(),
                            format!(
                                "{}\n\n{}",
                                context_header.text,
                                render_channel_text(text).text
                            ),
                        ];
                        for option in options {
                            args.extend(["--option".into(), render_channel_text(option).text]);
                        }
                        let output = self.executor.run(args).await?;
                        let caption = format!(
                            "{}\n\n{}",
                            context_header.text,
                            render_channel_text(text).text
                        );
                        return self
                            .poll_sent_ids(&message.conversation, &caption, &output.stdout)
                            .await;
                    }
                }
            }
        }
        self.send_rendered(&message.conversation, &rendered).await
    }

    fn subscribe(&self) -> mpsc::Receiver<InboundEvent> {
        self.inbound_rx
            .lock()
            .expect("iMessage subscription lock poisoned")
            .take()
            .expect("iMessage provider supports one inbound subscriber")
    }

    fn liveness(&self) -> super::ChannelLiveness {
        let value = self.watch_liveness();
        super::ChannelLiveness {
            active: value.active,
            started_at: value.started_at,
            last_receipt_at: value.last_receipt_at,
            last_receipt_rowid: value.last_receipt_rowid,
        }
    }
    fn health(&self) -> ChannelHealth {
        self.health
            .read()
            .expect("iMessage health lock poisoned")
            .clone()
    }

    async fn operator_presence(&self) -> OperatorPresence {
        match self.executor.operator_presence().await {
            Ok(presence) => presence,
            Err(error) => {
                log::warn!("could not read operator presence; delivering immediately: {error}");
                OperatorPresence::Away
            }
        }
    }

    async fn cleanup_question(&self, message: &ResolvedQuestionMessage) -> Result<(), String> {
        IMessageProvider::cleanup_question(self, message).await
    }
}

/// Classifies `imsg status --json`. Ready is the gate that lets a question go out as a
/// native poll, so it asserts poll capability specifically: a bridge that is connected
/// but cannot carry polls is Degraded, because the numbered-text floor is what it can
/// actually deliver. Absent IMCore advanced features - SIP re-enabled, or Messages
/// started without the bridge - are the silent-failure mode this classification names.
///
/// The schema is imsg's own; `cairn-core/tests/fixtures/imsg-status-0.13.4.json` holds a
/// payload captured from the real binary and is what the tests parse. An imsg upgrade
/// may move these keys, so recapture that fixture rather than guessing at key names.
fn parse_status(stdout: &str) -> ChannelHealth {
    let Ok(value) = serde_json::from_str::<Value>(stdout) else {
        return ChannelHealth::Unavailable {
            reason: "imsg status returned invalid JSON".into(),
        };
    };
    let advanced = bool_at(&value, &["advanced_features"]).unwrap_or(false)
        || bool_at(&value, &["v2_ready"]).unwrap_or(false);
    if advanced && reports_poll_support(&value) {
        return ChannelHealth::Ready;
    }
    ChannelHealth::Degraded {
        reason: degraded_reason(&value, advanced),
    }
}

/// True when the bridge advertises polls on either surface it reports them on: the
/// IMCore selectors it resolved, or the RPC methods it accepts.
fn reports_poll_support(value: &Value) -> bool {
    let selector = value
        .get("selectors")
        .and_then(|selectors| selectors.get("pollPayloadMessage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let rpc = value
        .get("rpc_methods")
        .and_then(Value::as_array)
        .is_some_and(|methods| {
            methods
                .iter()
                .filter_map(Value::as_str)
                .any(|method| method == "poll.send" || method == "messages.poll.send")
        });
    selector || rpc
}

/// Names which capability is missing in our own words. imsg's `message` field is quoted
/// as attributed context, never used as the reason itself: it describes the connection,
/// not the capability, so on its own it produced health strings that asserted
/// availability while the provider was refusing to send polls.
fn degraded_reason(value: &Value, advanced: bool) -> String {
    let version = string_at(value, &["version"]).unwrap_or_else(|| "unknown version".into());
    let sip = string_at(value, &["sip"]).unwrap_or_else(|| "unknown".into());
    let cause = if advanced {
        "bridge is connected but advertises no poll support"
    } else {
        "IMCore advanced features are unavailable"
    };
    let mut reason =
        format!("imsg {version} (SIP {sip}): {cause}; asks fall back to numbered text");
    if let Some(message) = string_at(value, &["message"]) {
        reason.push_str(&format!(" - imsg reports: {message}"));
    }
    reason
}

/// The identity of a conversation as imsg reports it: `guid` addresses `poll send`,
/// `id` addresses `history`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chat {
    pub guid: String,
    pub id: i64,
}

/// Finds the one-to-one chat for a handle in `imsg chats --json`, which emits one chat
/// object per line. Handles are normalized on both sides so a formatted number and its
/// canonical form address the same chat. A group that merely contains the handle is not
/// that conversation and is never matched.
fn find_chat(stdout: &str, conversation: &str) -> Option<Chat> {
    let wanted = normalize_handle(conversation);
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|chat| {
            if bool_at(chat, &["is_group", "isGroup"]).unwrap_or(false) {
                return false;
            }
            let identified = string_at(chat, &["identifier", "guid"])
                .is_some_and(|handle| normalize_handle(&handle) == wanted)
                || string_at(chat, &["guid"]).is_some_and(|guid| guid == conversation);
            let sole_participant = chat
                .get("participants")
                .and_then(Value::as_array)
                .is_some_and(|participants| {
                    participants.len() == 1
                        && participants
                            .iter()
                            .filter_map(Value::as_str)
                            .any(|handle| normalize_handle(handle) == wanted)
                });
            identified || sole_participant
        })
        .and_then(|chat| {
            Some(Chat {
                guid: string_at(&chat, &["guid"])?,
                id: i64_at(&chat, &["id", "chat_id", "rowid"])?,
            })
        })
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

/// Recovers the guid of a message imsg sent but did not report, by matching its text in
/// `imsg history --json` - which, like `watch` and `chats`, emits one message object per
/// line rather than a single document.
fn reconcile_history(stdout: &str, rendered: &str) -> Option<SentIds> {
    let rows: Vec<Value> = stdout
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let mut primary = None;
    let mut caption = None;
    for row in rows.iter().rev() {
        if !bool_at(row, &["is_from_me", "isFromMe"]).unwrap_or(false) {
            continue;
        }
        let text = string_at(row, &["text", "body"]).unwrap_or_default();
        let Some(guid) = string_at(row, &["guid", "message_guid", "messageGuid"]) else {
            continue;
        };
        if text == rendered || rendered.ends_with(&text) || text.ends_with(rendered) {
            if string_at(
                row,
                &[
                    "thread_originator_guid",
                    "threadOriginatorGuid",
                    "reply_to_guid",
                ],
            )
            .is_some()
            {
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
    } else if digits.len() == 11 && digits.starts_with('1') {
        digits[1..].to_string()
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

    /// Captured from `imsg status --json` on a bglab-mac bridge that was connected and
    /// sending polls (imsg 0.13.4, bridge_version 2). Health classification is parsed
    /// from this rather than from hand-written JSON: the imagined schema this parser
    /// used to look for was how a fully-ready bridge shipped as permanently Degraded.
    const REAL_STATUS: &str = include_str!("../../tests/fixtures/imsg-status-0.13.4.json");

    /// Shapes captured from the same bridge: `imsg chats --json` emits one chat per
    /// line, and `imsg poll send --json` answers with the balloon's `messageGuid` and
    /// no guid for the caption it sends afterward. Handles here are examples; the keys
    /// and framing are the binary's.
    const REAL_CHATS: &str = include_str!("../../tests/fixtures/imsg-chats-0.13.4.jsonl");
    const REAL_POLL_SEND: &str = include_str!("../../tests/fixtures/imsg-poll-send-0.13.4.json");
    const REAL_HISTORY: &str = include_str!("../../tests/fixtures/imsg-history-0.13.4.jsonl");
    const REAL_POLL_VOTE: &str =
        include_str!("../../tests/fixtures/imsg-watch-poll-vote-0.13.4.jsonl");

    /// The real payload with one field edited, so a not-ready case still differs from
    /// the real binary's output only in the way the scenario claims it does.
    fn status_with(mutate: impl FnOnce(&mut serde_json::Map<String, Value>)) -> String {
        let mut value: Value = serde_json::from_str(REAL_STATUS).expect("fixture is valid JSON");
        mutate(value.as_object_mut().expect("fixture is a JSON object"));
        value.to_string()
    }

    /// The captured history preserves the uppercase project key that was actually sent.
    /// Tests using the lowercase canonical message must mock the history produced by that
    /// send, because production reconciliation deliberately matches rendered text exactly.
    fn canonical_message_history() -> String {
        REAL_HISTORY.replacen("[CAIRN-3373", "[cairn-3373", 1)
    }

    struct FakeExecutor {
        results: Mutex<VecDeque<Result<CommandResult, String>>>,
        calls: Mutex<Vec<Vec<String>>>,
        watch_lines: Vec<String>,
    }

    impl FakeExecutor {
        fn new(outputs: impl IntoIterator<Item = impl Into<String>>) -> Self {
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
            started: oneshot::Sender<()>,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push(args);
            let _ = started.send(());
            for line in &self.watch_lines {
                lines.send(Ok(line.clone())).await.unwrap();
            }
            Ok(())
        }
    }

    struct FailingWatchExecutor;

    #[async_trait]
    impl IMessageExecutor for FailingWatchExecutor {
        async fn run(&self, _args: Vec<String>) -> Result<CommandResult, String> {
            unreachable!("failing watch test does not run commands")
        }

        async fn watch(
            &self,
            _args: Vec<String>,
            _lines: mpsc::Sender<Result<String, String>>,
            _started: oneshot::Sender<()>,
        ) -> Result<(), String> {
            Err("watch did not start".into())
        }
    }

    #[tokio::test]
    async fn failed_watch_retries_never_claim_an_active_connection() {
        let provider = Arc::new(IMessageProvider::new(
            Arc::new(FailingWatchExecutor),
            vec![],
        ));

        for _ in 0..2 {
            let (cursor_tx, _cursor_rx) = mpsc::channel(1);
            assert_eq!(
                provider.spawn_watch(41, cursor_tx).await.unwrap(),
                Err("watch did not start".into())
            );
            let liveness = provider.watch_liveness();
            assert!(!liveness.active);
            assert_eq!(liveness.started_at, None);
        }
    }

    #[test]
    fn remotely_placed_channel_never_probes_its_executor_for_presence() {
        assert_eq!(placed_presence_fallback(), OperatorPresence::Away);
    }

    #[test]
    fn presence_requires_recent_input_and_an_unlocked_screen() {
        assert_eq!(
            parse_operator_presence("0 0").unwrap(),
            OperatorPresence::Present
        );
        assert_eq!(
            parse_operator_presence("59 0").unwrap(),
            OperatorPresence::Present
        );
        assert_eq!(
            parse_operator_presence("60 0").unwrap(),
            OperatorPresence::Away
        );
        assert_eq!(
            parse_operator_presence("0 1").unwrap(),
            OperatorPresence::Away
        );
    }

    #[test]
    fn incomplete_presence_output_fails_closed_to_immediate_delivery() {
        assert!(parse_operator_presence("").is_err());
        assert!(parse_operator_presence("0").is_err());
        assert!(parse_operator_presence("0 maybe").is_err());
    }

    #[test]
    fn channel_text_removes_markdown_without_losing_deep_links() {
        let markdown = "# **cairn-3550** stream update\n\nUse `render_channel_text`:\n* first item\n* [Open in Cairn](cairn://p/cairn/3550)\n\n_Ready_.";
        assert_eq!(
            render_channel_text(markdown).text,
            "cairn-3550 stream update\n\nUse render_channel_text:\n\n- first item\n- cairn://p/cairn/3550\n\nReady."
        );
    }

    /// The numbered floor is the operator's whole affordance for answering by
    /// number, and the reply parser resolves a number back to an option by
    /// position, so the ordinals have to survive rendering.
    #[test]
    fn channel_text_keeps_ordered_list_numbering_and_nests_under_a_bullet_list() {
        assert_eq!(
            render_channel_text(&crate::channels::render_text_floor(&OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 0,
                text: "Which path?".into(),
                options: vec![
                    super::super::AskOption {
                        label: "Legacy".into(),
                        description: None
                    },
                    super::super::AskOption {
                        label: "New".into(),
                        description: None
                    },
                ],
            }))
            .text,
            "Which path?\n\n1. Legacy\n2. New\n\nReply to this message with a number or your answer."
        );
        assert_eq!(
            render_channel_text("- outer\n  1. first\n  2. second\n- after").text,
            "- outer\n  1. first\n  2. second\n- after"
        );
    }

    #[test]
    fn channel_text_preserves_fenced_code_and_literal_underscores() {
        assert_eq!(
            render_channel_text("```rust\nlet raw_value = **literal**;\n```\nfoo_bar_baz").text,
            "let raw_value = **literal**;\n\nfoo_bar_baz"
        );
    }

    #[test]
    fn channel_text_structurally_renders_nested_emphasis() {
        let rendered = render_channel_text("**bold and *italic***");
        assert_eq!(rendered.text, "bold and italic");
        assert_eq!(rendered.formats.len(), 2);
        assert!(rendered
            .formats
            .iter()
            .any(|format| format.styles == ["bold"]));
        assert!(rendered
            .formats
            .iter()
            .any(|format| format.styles == ["italic"]));
    }

    fn message() -> OutboundMessage {
        message_with(&["Legacy", "New"])
    }

    fn message_with(labels: &[&str]) -> OutboundMessage {
        OutboundMessage {
            intent_id: "intent".into(),
            conversation: "+15551234567".into(),
            initiated_by: crate::channels::OutboundInitiator::CairnPush,
            context_header: "[cairn-3373 · builder]".into(),
            ask: OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 0,
                text: "Which path?".into(),
                options: labels
                    .iter()
                    .map(|label| super::super::AskOption {
                        label: (*label).into(),
                        description: None,
                    })
                    .collect(),
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
        let liveness = provider.watch_liveness();
        assert!(liveness.started_at.is_some());
        assert!(liveness.last_receipt_at.is_some());
        assert_eq!(liveness.last_receipt_rowid, Some(42));
    }

    #[tokio::test]
    async fn degraded_health_sends_text_and_reconciles_guid_from_history() {
        let bridgeless = status_with(|status| {
            status.insert("advanced_features".into(), Value::Bool(false));
            status.insert("v2_ready".into(), Value::Bool(false));
            status.insert("sip".into(), Value::String("enabled".into()));
        });
        let fake = Arc::new(FakeExecutor::new([
            bridgeless,
            "{}".into(),
            REAL_CHATS.into(),
            canonical_message_history(),
        ]));
        let provider = IMessageProvider::new(fake.clone(), vec!["+15551234567".into()]);
        assert!(matches!(
            provider.refresh_health().await,
            ChannelHealth::Degraded { .. }
        ));
        assert_eq!(
            provider.send(&message()).await.unwrap().primary_guid,
            "63E47F81-B45E-49A5-B241-E94926FAEDEC"
        );
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[1][0], "send");
        assert_eq!(calls[2][0], "chats");
        // `history` takes a chat rowid; the handle `send` accepts is rejected here.
        assert_eq!(&calls[3][..4], &["history", "--json", "--chat-id", "2"]);
    }

    #[tokio::test]
    async fn degraded_bridge_sends_clean_markdown_floor() {
        let bridgeless = status_with(|status| {
            status.insert("advanced_features".into(), Value::Bool(false));
            status.insert("v2_ready".into(), Value::Bool(false));
            if let Some(Value::Array(methods)) = status.get_mut("rpc_methods") {
                methods.retain(|method| method.as_str() != Some("send.rich"));
            }
        });
        let fake = Arc::new(FakeExecutor::new([
            bridgeless,
            r#"{"guid":"plain-guid"}"#.to_string(),
        ]));
        let provider = IMessageProvider::new(fake.clone(), vec![]);
        provider.refresh_health().await;
        let mut message = message();
        message.context_header.clear();
        message.ask = OutboundAsk::Notify {
            text: "**Bold** and `inline_code`\n- [Open](cairn://p/cairn/3550)".into(),
        };

        provider.send(&message).await.unwrap();

        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[1][0], "send");
        assert!(calls[1]
            .windows(2)
            .any(|pair| { pair == ["--text", "Bold and inline_code\n\n- cairn://p/cairn/3550"] }));
    }

    #[tokio::test]
    async fn rich_send_does_not_depend_on_poll_support() {
        let rich_without_polls = status_with(|status| {
            if let Some(Value::Object(selectors)) = status.get_mut("selectors") {
                selectors.insert("pollPayloadMessage".into(), Value::Bool(false));
            }
            if let Some(Value::Array(methods)) = status.get_mut("rpc_methods") {
                methods.retain(|method| {
                    !matches!(method.as_str(), Some("poll.send" | "messages.poll.send"))
                });
            }
        });
        let fake = Arc::new(FakeExecutor::new([
            rich_without_polls.as_str(),
            REAL_CHATS,
            r#"{"guid":"rich-guid"}"#,
        ]));
        let provider = IMessageProvider::new(fake.clone(), vec![]);
        assert!(matches!(
            provider.refresh_health().await,
            ChannelHealth::Degraded { .. }
        ));
        let mut message = message();
        message.context_header.clear();
        message.ask = OutboundAsk::Notify {
            text: "**Bold** and *italic*".into(),
        };

        provider.send(&message).await.unwrap();

        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[2][0], "send-rich");
        assert!(calls[2]
            .windows(2)
            .any(|pair| pair == ["--text", "Bold and italic"]));
        let format = calls[2]
            .windows(2)
            .find(|pair| pair[0] == "--format")
            .map(|pair| pair[1].as_str())
            .unwrap();
        assert!(format.contains("\"styles\":[\"bold\"]"));
        assert!(format.contains("\"styles\":[\"italic\"]"));
    }

    #[tokio::test]
    async fn ready_health_sends_poll_with_argument_boundaries() {
        let poll_caption = r#"{"guid":"caption-guid","chat_id":2,"is_from_me":true,"text":"[cairn-3373 · builder]\n\nWhich path?"}"#;
        let fake = Arc::new(FakeExecutor::new([
            REAL_STATUS,
            REAL_CHATS,
            REAL_POLL_SEND,
            REAL_CHATS,
            poll_caption,
        ]));
        let provider = IMessageProvider::new(fake.clone(), vec!["+15551234567".into()]);
        assert_eq!(provider.refresh_health().await, ChannelHealth::Ready);
        let mut markdown_message = message_with(&["**Legacy**", "_New_"]);
        markdown_message.context_header = "**[cairn-3373 · builder]**".into();
        if let OutboundAsk::Question { text, .. } = &mut markdown_message.ask {
            *text = "Which `path`?".into();
        }
        let ids = provider.send(&markdown_message).await.unwrap();
        // imsg sends the caption itself and reports only the balloon's guid, so the ask
        // binds to the poll a vote actually arrives against.
        assert_eq!(ids.primary_guid, "3892DF46-42FE-41FB-960B-292F16EF7FB0");
        assert_eq!(ids.caption_guid.as_deref(), Some("caption-guid"));
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[1][0], "chats");
        assert_eq!(&calls[2][..2], &["poll", "send"]);
        assert!(calls[2].contains(&"--json".to_string()));
        // `poll send` addresses a chat guid; the bare handle `send` takes is rejected.
        assert!(calls[2]
            .windows(2)
            .any(|pair| pair == ["--chat", "any;-;+15551234567"]));
        assert!(calls[2]
            .windows(2)
            .any(|pair| pair == ["--option", "Legacy"]));
        assert!(calls[2].windows(2).any(|pair| pair == ["--option", "New"]));
        assert!(calls[2]
            .windows(2)
            .any(|pair| { pair == ["--question", "[cairn-3373 · builder]\n\nWhich path?"] }));
    }

    #[tokio::test]
    async fn ready_health_sends_permission_as_approve_deny_poll() {
        let poll_caption = r#"{"guid":"caption-guid","chat_id":2,"is_from_me":true,"text":"[cairn-3373 · builder]\n\nReadable permission"}"#;
        let fake = Arc::new(FakeExecutor::new([
            REAL_STATUS,
            REAL_CHATS,
            REAL_POLL_SEND,
            REAL_CHATS,
            poll_caption,
        ]));
        let provider = IMessageProvider::new(fake.clone(), vec!["+15551234567".into()]);
        assert_eq!(provider.refresh_health().await, ChannelHealth::Ready);
        let mut permission = message();
        permission.ask = OutboundAsk::Permission {
            request_id: "permission-request".into(),
            summary: "Readable permission".into(),
        };

        provider.send(&permission).await.unwrap();

        let calls = fake.calls.lock().unwrap();
        assert_eq!(&calls[2][..2], &["poll", "send"]);
        let options = calls[2]
            .windows(2)
            .filter(|pair| pair[0] == "--option")
            .map(|pair| pair[1].as_str())
            .collect::<Vec<_>>();
        assert_eq!(options, vec!["Approve", "Deny"]);
        assert!(calls[2].windows(2).any(|pair| pair
            == [
                "--question",
                "[cairn-3373 · builder]\n\nReadable permission"
            ]));
    }

    /// A handle imsg has no chat for has nothing to hold a balloon, so the first ask to
    /// a new recipient takes the text floor - and that send is what creates the chat,
    /// which is why the guid is still recoverable afterward.
    #[tokio::test]
    async fn a_chat_that_does_not_exist_yet_takes_the_text_floor() {
        let fake = Arc::new(FakeExecutor::new([
            REAL_STATUS.into(),
            String::new(),
            "{}".into(),
            REAL_CHATS.into(),
            canonical_message_history(),
        ]));
        let provider = IMessageProvider::new(fake.clone(), vec!["+15551234567".into()]);
        assert_eq!(provider.refresh_health().await, ChannelHealth::Ready);

        let ids = provider.send(&message()).await.unwrap();

        assert_eq!(ids.primary_guid, "63E47F81-B45E-49A5-B241-E94926FAEDEC");
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[1][0], "chats");
        assert_eq!(calls[2][0], "send");
    }

    #[test]
    fn chat_lookup_normalizes_handles_and_never_matches_a_group() {
        assert_eq!(
            find_chat(REAL_CHATS, " +1 (555) 123-4567 "),
            Some(Chat {
                guid: "any;-;+15551234567".into(),
                id: 2,
            })
        );
        assert_eq!(find_chat(REAL_CHATS, "+15557654321"), None);
    }

    /// A vote event names its own option; the poll that would have populated the option
    /// map was sent by Cairn and never comes back through the watch stream.
    #[test]
    fn a_real_poll_vote_resolves_to_the_option_label_not_its_uuid() {
        let provider = IMessageProvider::new(
            Arc::new(FakeExecutor::new(Vec::<String>::new())),
            vec!["+15551234567".into()],
        );

        let event = provider.ingest_watch_line(REAL_POLL_VOTE.trim()).unwrap();

        let Some((
            75,
            InboundEvent::Selections {
                bound_guid,
                changes,
                ..
            },
        )) = event
        else {
            panic!("a real vote must resolve to selections, got {event:?}");
        };
        assert_eq!(bound_guid, "3892DF46-42FE-41FB-960B-292F16EF7FB0");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].option_text, "New");
        assert!(changes[0].selected);
    }

    /// Messages renders a poll balloon only with at least two options, so a one-option
    /// ask has to reach the operator as text instead of failing the delivery.
    #[tokio::test]
    async fn single_option_ask_takes_the_text_floor_a_poll_cannot_hold() {
        let floor = r#"{"guid":"m1","chat_id":2,"is_from_me":true,"text":"[cairn-3373 · builder]\n\nWhich path?\n\n1. New\n\nReply to this message with a number or your answer."}"#;
        let fake = Arc::new(FakeExecutor::new([REAL_STATUS, "{}", REAL_CHATS, floor]));
        let provider = IMessageProvider::new(fake.clone(), vec!["+15551234567".into()]);
        assert_eq!(provider.refresh_health().await, ChannelHealth::Ready);

        let ids = provider.send(&message_with(&["New"])).await.unwrap();

        assert_eq!(ids.primary_guid, "m1");
        assert_eq!(fake.calls.lock().unwrap()[1][0], "send");
    }

    #[tokio::test]
    async fn failed_unsend_falls_back_to_a_resolution_receipt_within_edit_window() {
        let fake = Arc::new(FakeExecutor {
            results: Mutex::new(VecDeque::from([
                Ok(CommandResult {
                    stdout: REAL_CHATS.into(),
                    stderr: String::new(),
                }),
                Err("unsend window rejected by recipient".into()),
                Ok(CommandResult {
                    stdout: "{}".into(),
                    stderr: String::new(),
                }),
            ])),
            calls: Mutex::new(Vec::new()),
            watch_lines: Vec::new(),
        });
        let provider = IMessageProvider::new(fake.clone(), vec![]);
        provider
            .cleanup_question(&ResolvedQuestionMessage {
                conversation: "+15551234567".into(),
                provider_guid: "message-guid".into(),
                caption_guid: None,
                sent_at: chrono::Utc::now().timestamp() - 30,
                receipt: "✓ answered: Ship it".into(),
            })
            .await
            .unwrap();
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[1][0], "unsend");
        assert_eq!(calls[2][0], "edit");
        assert!(calls[2]
            .windows(2)
            .any(|pair| pair == ["--new-text", "✓ answered: Ship it"]));
    }

    #[tokio::test]
    async fn recent_resolution_unsends_poll_and_caption() {
        let fake = Arc::new(FakeExecutor::new([REAL_CHATS, "{}", "{}"]));
        let provider = IMessageProvider::new(fake.clone(), vec![]);
        provider
            .cleanup_question(&ResolvedQuestionMessage {
                conversation: "+15551234567".into(),
                provider_guid: "poll-guid".into(),
                caption_guid: Some("caption-guid".into()),
                sent_at: chrono::Utc::now().timestamp(),
                receipt: "✓ answered: Ship it".into(),
            })
            .await
            .unwrap();
        let calls = fake.calls.lock().unwrap();
        assert_eq!(
            calls[1],
            vec![
                "unsend",
                "--chat",
                "any;-;+15551234567",
                "--message",
                "poll-guid"
            ]
        );
        assert_eq!(
            calls[2],
            vec![
                "unsend",
                "--chat",
                "any;-;+15551234567",
                "--message",
                "caption-guid"
            ]
        );
    }

    #[tokio::test]
    async fn edit_budget_stops_before_clients_silently_drop_updates() {
        let outputs = std::iter::repeat_n([REAL_CHATS, "{}"], MAX_PROPAGATING_EDITS as usize)
            .flatten()
            .collect::<Vec<_>>();
        let fake = Arc::new(FakeExecutor::new(outputs));
        let provider = IMessageProvider::new(fake.clone(), vec![]);
        for version in 1..=MAX_PROPAGATING_EDITS {
            provider
                .edit("+15551234567", "message-guid", &format!("v{version}"))
                .await
                .unwrap();
        }
        let error = provider
            .edit("+15551234567", "message-guid", "v6")
            .await
            .unwrap_err();
        assert!(error.contains("send a fresh status message"));
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls.len(), MAX_PROPAGATING_EDITS as usize * 2);
        assert_eq!(&calls[1][..2], &["edit", "--chat"]);
        assert!(calls[1].windows(2).any(|pair| pair == ["--new-text", "v1"]));
    }

    #[test]
    fn real_connected_bridge_is_ready_on_either_poll_signal() {
        assert_eq!(parse_status(REAL_STATUS), ChannelHealth::Ready);

        let selectors_only = status_with(|status| {
            status.remove("rpc_methods");
        });
        assert_eq!(parse_status(&selectors_only), ChannelHealth::Ready);

        let rpc_only = status_with(|status| {
            status.remove("selectors");
        });
        assert_eq!(parse_status(&rpc_only), ChannelHealth::Ready);
    }

    #[test]
    fn missing_capability_degrades_with_a_reason_that_does_not_assert_availability() {
        let bridgeless = status_with(|status| {
            status.insert("advanced_features".into(), Value::Bool(false));
            status.insert("v2_ready".into(), Value::Bool(false));
            status.insert("sip".into(), Value::String("enabled".into()));
        });
        let ChannelHealth::Degraded { reason } = parse_status(&bridgeless) else {
            panic!("a bridge without IMCore features cannot send polls");
        };
        assert!(reason.starts_with("imsg 0.13.4 (SIP enabled)"), "{reason}");
        assert!(
            reason.contains("IMCore advanced features are unavailable"),
            "{reason}"
        );
        assert!(
            reason.contains("imsg reports: Connected to Messages.app"),
            "{reason}"
        );

        let pollless = status_with(|status| {
            status.remove("selectors");
            status.remove("rpc_methods");
        });
        let ChannelHealth::Degraded { reason } = parse_status(&pollless) else {
            panic!("advanced features alone do not prove polls can be sent");
        };
        assert!(reason.contains("advertises no poll support"), "{reason}");
    }

    #[test]
    fn parses_allowlisted_reply_vote_mutations_and_echo_guard() {
        let provider = IMessageProvider::new(
            Arc::new(FakeExecutor::new(Vec::<String>::new())),
            vec!["+14155550123".into()],
        );
        let mutation = r#"{"rowid":10,"sender":"+14155550123","poll":{"original_guid":"poll","options":[{"id":"a","text":"Alpha"}],"votes":[]}}"#;
        assert_eq!(provider.ingest_watch_line(mutation).unwrap(), None);
        let vote = r#"{"rowid":11,"sender":"+14155550123","poll":{"original_guid":"poll","options":[{"id":"a","text":"Alpha"}],"votes":[{"participant":"+14155550123","option_id":"a"}]}}"#;
        assert!(matches!(
            provider.ingest_watch_line(vote).unwrap(),
            Some((
                11,
                InboundEvent::Selections { changes, .. }
            )) if changes.len() == 1
                && changes[0].option_text == "Alpha"
                && changes[0].selected
        ));
        assert_eq!(provider.ingest_watch_line(vote).unwrap(), None);

        let two_changes = r#"{"rowid":12,"sender":"+14155550123","poll":{"original_guid":"poll","options":[{"id":"a","text":"Alpha"},{"id":"b","text":"Beta"},{"id":"c","text":"Gamma"}],"votes":[{"participant":"+14155550123","option_id":"b"},{"participant":"+14155550123","option_id":"c"}]}}"#;
        assert!(matches!(
            provider.ingest_watch_line(two_changes).unwrap(),
            Some((
                12,
                InboundEvent::Selections { changes, .. }
            )) if changes.len() == 3
                && changes.iter().filter(|change| change.selected).count() == 2
                && changes.iter().filter(|change| !change.selected).count() == 1
        ));
        let reply =
            r#"{"rowid":13,"sender":"+14155550123","text":"2","thread_originator_guid":"poll"}"#;
        assert!(matches!(
            provider.ingest_watch_line(reply).unwrap(),
            Some((13, InboundEvent::Reply { .. }))
        ));
        let echo = r#"{"rowid":13,"sender":"+14155550123","text":"mine","is_from_me":true}"#;
        assert_eq!(provider.ingest_watch_line(echo).unwrap(), None);
    }

    #[test]
    fn normalizes_handles_and_parses_reply_numbers() {
        let allowlist = vec![" +1 (415) 555-0123 ".into(), "USER@Example.COM".into()];
        assert!(is_allowlisted("+14155550123", &allowlist));
        assert!(is_allowlisted("415-555-0123", &allowlist));
        assert!(is_allowlisted(" user@example.com ", &allowlist));
        assert!(!is_allowlisted("+14155559999", &allowlist));
        assert_eq!(parse_reply_number(" 2. New", 3), Some(1));
        assert_eq!(parse_reply_number("4", 3), None);
    }

    #[test]
    fn unknown_sender_diagnostic_names_both_sides_of_the_allowlist() {
        assert_eq!(
            sender_mismatch_diagnostic("", &["+1 (415) 555-0123".into()]),
            "iMessage inbound sender `<missing>` does not match configured operator handle(s) `+1 (415) 555-0123`"
        );
        assert_eq!(
            sender_mismatch_diagnostic("stranger@example.com", &[]),
            "iMessage inbound sender `stranger@example.com` does not match configured operator handle(s) `<empty>`"
        );
    }
}
