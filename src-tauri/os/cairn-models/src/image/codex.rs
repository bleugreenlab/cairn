//! The image verb over Codex: each image is drawn by a worker thread that
//! calls Codex's own image tool (`image_gen.imagegen`) once.
//!
//! A worker is a tool-free Codex thread given back exactly one native tool
//! ([`CodexNativeTools::ImageGeneration`]) on an app-server signed in to
//! ChatGPT. Its instructions are to pass the composed prompt to the tool
//! verbatim and answer `DONE`, or `FAILED: ` and the tool's error, which is
//! the only place a moderation block's reason appears. The image itself comes
//! back as the thread's `imageGeneration` item, read by the one-shot fold
//! ([`OneShotAllow::ImageGeneration`]).
//!
//! One turn draws one image: asked for several, Codex calls the tool
//! serially, so `count` images are `count` threads in parallel on the one
//! server. A thread whose turn ends without an image is retried once on a
//! fresh thread within the call's deadline, which bounds the whole call,
//! thread and turn setup included, and no turn is started once the call has
//! answered. Codex also saves every image under its home; each worker removes
//! its thread's directory, and follows a turn that ended badly to its real end
//! to remove any image it still writes (see [`DRAIN_QUIET`] for the one
//! bounded residual).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::codex::app_server::AppServerClient;
use crate::codex::thread_router::{ThreadRoute, ThreadRouter};
use crate::image::http::HttpImages;
use crate::image_wire::ImageWireError;
use crate::interface::image::{
    CodexDrawOutcome, CodexImageReply, CodexImageRoute, CodexImageTurn, GeneratedImage, ImageBody,
    ImageError, ImagePost, ImageReply, ImageRoute, ImageTransport, REFUSAL_CODES,
};
use crate::interface::session::{
    ApprovalMode, CodexNativeTools, CodexServer, CodexSpec, NativeGoal, ProviderSpec, SecretEnv,
    SessionEvents, SessionHost, SessionProvider, SessionSpec, SessionStart,
};
use crate::session::codex::CodexAppServer;
use crate::session::one_shot::{await_one_shot_with, OneShotAllow, OneShotError, OneShotReply};
use crate::types::catalog::{DiscoveredModel, Modalities, Modality, ServedVerb, Verb, Wire};
use crate::types::image::ImageCapabilities;
use crate::types::usage::MeteredUsage;
use crate::wire::codex_app_server::{ImageGenerationFailure, ImageGenerationItem, Notification};
use crate::wire::{MessageContent, MessageImage};
use crossbeam_channel::Receiver;

/// A worker thread's base instructions: the probe's (cairn/4905), with the
/// tool called exactly once.
pub const WORKER_INSTRUCTIONS: &str = "You are an image worker. Your only job is to call the \
image_gen.imagegen tool exactly once, passing the text inside <prompt> verbatim as `prompt`. \
When the user message attaches images, set num_last_images_to_include to the number of attached \
images. Never ask questions and never reply in prose; after the tool returns, reply with the \
single word DONE. If the tool reports an error, reply with FAILED: followed by the tool error \
text verbatim.";

/// The worker's reasoning effort when the call's extras name none.
pub const DEFAULT_WORKER_EFFORT: &str = "low";

/// The one extra the Codex wire takes: there is no body for any other to
/// ride in.
const EFFORT_EXTRA: &str = "reasoning_effort";

/// The id Codex's image offer is listed under: the image model that draws,
/// not the worker that calls it.
pub const CODEX_IMAGE_MODEL: &str = "gpt-image-2";

/// Codex's image offer as a catalog row: it serves `image` alone, over the
/// Codex app-server, so no session or completion is ever placed on it. A host
/// lists it beside its Codex chat models; Codex's own model listing does not
/// contain it.
pub fn discovered_model() -> DiscoveredModel {
    DiscoveredModel {
        id: CODEX_IMAGE_MODEL.to_string(),
        model: CODEX_IMAGE_MODEL.to_string(),
        display_name: "GPT Image (Codex)".to_string(),
        description: Some(
            "Codex's built-in image generation, drawn under a ChatGPT sign-in on a paid plan"
                .to_string(),
        ),
        hidden: false,
        is_default: false,
        default_reasoning_effort: None,
        supported_reasoning_efforts: Vec::new(),
        context_window: None,
        canonical_slug: None,
        serving_account_ids: Vec::new(),
        pricing: None,
        supported_parameters: Vec::new(),
        router: false,
        modalities: Some(Modalities {
            input: vec![Modality::Text, Modality::Image],
            output: vec![Modality::Image],
        }),
        serves: Some(vec![ServedVerb {
            verb: Verb::Image(ImageCapabilities::codex_image_gen()),
            wire: Wire::CodexAppServer,
        }]),
        // An image offer has no token budget to meter.
        tokenizer: None,
    }
}

/// What the worker's turn says: the composed prompt, for the tool verbatim.
pub fn turn_text(prompt: &str) -> String {
    format!("<prompt>{prompt}</prompt>")
}

/// The worker's effort from the call's `codex-app-server` extras.
pub fn worker_effort(extras: &Map<String, Value>) -> Result<String, ImageError> {
    if let Some(key) = extras.keys().find(|key| *key != EFFORT_EXTRA) {
        return Err(ImageError::InvalidRequest(format!(
            "the codex-app-server image wire takes only `{EFFORT_EXTRA}` in its extras; '{key}' has no body to ride in"
        )));
    }
    match extras.get(EFFORT_EXTRA) {
        None => Ok(DEFAULT_WORKER_EFFORT.to_string()),
        Some(Value::String(effort)) if !effort.trim().is_empty() => Ok(effort.clone()),
        Some(other) => Err(ImageError::InvalidRequest(format!(
            "`{EFFORT_EXTRA}` is an effort name like \"low\", not {other}"
        ))),
    }
}

/// The images and usage the workers produced. Outcomes are judged in order
/// and the first failure decides the call: no partial result is returned.
pub(super) fn read_reply(
    provider: &str,
    reply: &CodexImageReply,
) -> Result<(Vec<GeneratedImage>, MeteredUsage), ImageError> {
    let mut images = Vec::new();
    for draw in &reply.draws {
        images.push(read_draw(provider, draw)?);
    }
    let turns: Vec<_> = reply
        .draws
        .iter()
        .flat_map(|draw| draw.usage.iter().cloned())
        .collect();
    let usage = super::usage::codex(&turns, images.len());
    Ok((images, usage))
}

fn read_draw(provider: &str, draw: &CodexDrawOutcome) -> Result<GeneratedImage, ImageError> {
    let said = draw.text.trim();
    let Some(image) = &draw.image else {
        let said = if said.is_empty() {
            String::new()
        } else {
            format!(" (it answered: {said})")
        };
        return Err(ImageError::Upstream(format!(
            "the Codex worker did not call its image tool{said}; Codex offers it only to a ChatGPT sign-in on a paid plan"
        )));
    };
    if image.is_completed() {
        if image.result.is_empty() {
            return Err(ImageError::InvalidResponse(format!(
                "{provider} completed an image with no bytes"
            )));
        }
        return super::generated(&image.result, None, None, image.revised_prompt.clone());
    }
    if let Some(ImageGenerationFailure::UsageLimitExceeded {
        limit_id,
        resets_at,
    }) = &image.failure
    {
        let resets = resets_at
            .and_then(|at| chrono::DateTime::from_timestamp(at, 0))
            .map(|at| format!("; it resets at {}", at.to_rfc3339()))
            .unwrap_or_default();
        return Err(ImageError::Quota {
            message: format!("{provider}: the {limit_id} limit is used up{resets}"),
        });
    }
    // The tool's own error reaches only the worker, which echoes it.
    let upstream = said
        .strip_prefix("FAILED:")
        .map(str::trim)
        .filter(|text| !text.is_empty());
    match upstream {
        Some(text) if REFUSAL_CODES.iter().any(|code| text.contains(code)) => {
            Err(ImageError::Refused {
                message: format!("{provider}: {text}"),
            })
        }
        Some(text) => Err(ImageError::Upstream(format!(
            "{provider}: the image tool failed: {text}"
        ))),
        None => Err(ImageError::Upstream(format!(
            "{provider}: the image tool failed ({}) and the worker gave no reason{}",
            image.status,
            if said.is_empty() {
                String::new()
            } else {
                format!("; it answered: {said}")
            }
        ))),
    }
}

/// How a worker turn that did not end cleanly (a failed start, a timeout, a
/// closed server, a forbidden tool) is followed after the call has let it go:
/// its thread stays routed, and each image that still completes on it is
/// removed, until the turn's own end (`turn/completed` or `turn/aborted`),
/// the server's death, `DRAIN_QUIET` of silence, or `DRAIN_LIMIT` in all.
/// Silence is the one residual: a turn that says nothing for that long and
/// then writes an image leaves that copy, and the worker logs it.
#[cfg(not(test))]
const DRAIN_QUIET: Duration = Duration::from_secs(120);
#[cfg(not(test))]
const DRAIN_LIMIT: Duration = Duration::from_secs(15 * 60);
#[cfg(test)]
const DRAIN_QUIET: Duration = Duration::from_secs(1);
#[cfg(test)]
const DRAIN_LIMIT: Duration = Duration::from_secs(5);

/// The image transport a host holds for every route: Codex bodies run as
/// worker threads on `server`, and every other body goes to `http`.
///
/// The server's process holds an access token only, so its owner answers its
/// token refresh through the router's scoped handler: Cairn's pool, or a
/// host's own over [`crate::codex::token_requests::token_request_reply`].
pub struct CodexImages {
    pub session_host: Arc<dyn SessionHost>,
    /// A server signed in (chatgptAuthTokens) as the account the call is
    /// routed to; every image thread opens on it, routed by its router.
    pub server: Arc<ThreadRouter>,
    /// The server's `CODEX_HOME`, on this machine. Codex saves every image
    /// under its `generated_images/<thread>/`; each worker removes its
    /// thread's directory however its turn ended, so the facet keeps nothing.
    pub codex_home: PathBuf,
    pub http: HttpImages,
}

impl ImageTransport for CodexImages {
    fn post(&self, post: ImagePost<'_>) -> Result<ImageReply, ImageWireError> {
        match (&post.body, post.route) {
            (ImageBody::Codex(turn), ImageRoute::Codex(route)) => {
                self.draw(route, turn, post.timeout).map(ImageReply::Codex)
            }
            (ImageBody::Codex(_), _) => Err(ImageWireError::InvalidRequest(
                "a Codex image body needs the Codex route".to_string(),
            )),
            _ => self.http.post(post),
        }
    }
}

impl CodexImages {
    /// `turn.count` images, each on a worker thread of its own, the whole
    /// call bounded by one deadline. The workers are not joined: a worker
    /// still opening its thread or waiting on its turn when the deadline
    /// passes is left to finish, and to clean up, on its own, so a slow
    /// app-server request (each waits up to the client's own timeout) never
    /// holds the call past its deadline.
    fn draw(
        &self,
        route: &CodexImageRoute,
        turn: &CodexImageTurn,
        timeout: Duration,
    ) -> Result<CodexImageReply, ImageWireError> {
        let deadline = Instant::now() + timeout;
        let abandoned = Arc::new(AtomicBool::new(false));
        let worker = Worker {
            session_host: self.session_host.clone(),
            server: self.server.clone(),
            codex_home: self.codex_home.clone(),
            route: route.clone(),
            turn: Arc::new(turn.clone()),
            deadline,
            abandoned: abandoned.clone(),
        };
        let count = turn.count as usize;
        let (done, outcomes) = crossbeam_channel::unbounded();
        for index in 0..count {
            let worker = worker.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let _ = done.send((index, worker.draw_one()));
            });
        }
        drop(done);
        let mut draws: Vec<Option<CodexDrawOutcome>> = vec![None; count];
        let gave_up = |error: ImageWireError| {
            abandoned.store(true, Ordering::SeqCst);
            Err(error)
        };
        while draws.iter().any(Option::is_none) {
            match outcomes.recv_deadline(deadline) {
                Ok((index, Ok(outcome))) => draws[index] = Some(outcome),
                Ok((_, Err(error))) => return gave_up(error),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    return gave_up(ImageWireError::Timeout)
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    return gave_up(ImageWireError::Transport(
                        "a Codex image worker ended without an outcome".to_string(),
                    ))
                }
            }
        }
        Ok(CodexImageReply {
            draws: draws.into_iter().flatten().collect(),
        })
    }
}

/// Everything one image's worker needs, owned, so it can outlive the call.
#[derive(Clone)]
struct Worker {
    session_host: Arc<dyn SessionHost>,
    server: Arc<ThreadRouter>,
    codex_home: PathBuf,
    route: CodexImageRoute,
    turn: Arc<CodexImageTurn>,
    deadline: Instant,
    /// Set once the call has answered without this worker (its deadline
    /// passed, or another image failed): no retry is started after it.
    abandoned: Arc<AtomicBool>,
}

/// How one worker turn ended, as far as the draw cares.
enum Turn {
    Answered {
        reply: Box<OneShotReply>,
        images: Vec<ImageGenerationItem>,
    },
    /// The turn failed without an answer: worth one retry.
    Missed(String),
}

impl Worker {
    /// One image: a worker turn, retried once on a fresh thread if it ends
    /// without one.
    fn draw_one(&self) -> Result<CodexDrawOutcome, ImageWireError> {
        let mut usage = Vec::new();
        let mut text = String::new();
        for _attempt in 0..2 {
            if self.abandoned.load(Ordering::SeqCst) {
                return Err(ImageWireError::Timeout);
            }
            match self.worker_turn()? {
                Turn::Answered { reply, images } => {
                    usage.extend(reply.usage);
                    text = reply.text;
                    // A worker that called the tool again drew images nobody
                    // asked for; the first is the one drawn for this call.
                    if let Some(image) = images.into_iter().next() {
                        return Ok(CodexDrawOutcome {
                            image: Some(image),
                            text,
                            usage,
                        });
                    }
                }
                Turn::Missed(reason) => text = reason,
            }
        }
        Ok(CodexDrawOutcome {
            image: None,
            text,
            usage,
        })
    }

    /// What a failure at `stage` is, given the deadline: past it, the call
    /// has timed out, whatever the request said.
    fn failed(&self, stage: &str, error: String) -> ImageWireError {
        if Instant::now() >= self.deadline {
            ImageWireError::Timeout
        } else {
            ImageWireError::Transport(format!("{stage}: {error}"))
        }
    }

    /// Open a worker thread, route it, start its turn, fold it until the
    /// deadline, and remove whatever Codex saved for it.
    fn worker_turn(&self) -> Result<Turn, ImageWireError> {
        if Instant::now() >= self.deadline {
            return Err(ImageWireError::Timeout);
        }
        let spec = SessionSpec {
            start: SessionStart::New {
                session_id: uuid::Uuid::new_v4().to_string(),
            },
            model: self.route.worker_model.clone(),
            effort: Some(self.turn.effort.clone()),
            approval: ApprovalMode::Deny,
            output_schema: None,
            cwd: std::env::temp_dir().to_string_lossy().to_string(),
            env: Default::default(),
            mcp: None,
            secrets: SecretEnv::new(),
            account_id: None,
            provider: ProviderSpec::Codex(CodexSpec {
                server: CodexServer::Shared(self.server.clone()),
                base_instructions: self.turn.instructions.clone(),
                developer_instructions: None,
                service_tier: None,
                native_goal: NativeGoal::Leave,
                native_tools: CodexNativeTools::ImageGeneration,
                resume_fallback: None,
            }),
        };
        let mut launched = CodexAppServer
            .open(self.session_host.as_ref(), spec)
            .map_err(|error| self.failed("could not open a Codex image thread", error.into()))?;
        let thread_id = launched.facts.backend_session_id.clone().ok_or_else(|| {
            ImageWireError::Transport("Codex opened an image thread without an id".to_string())
        })?;
        // From here this worker owns whatever Codex saves for the thread.
        // Routed before the turn starts, so none of its messages are dropped;
        // `watch` keeps the channel to follow a turn that ends badly.
        let thread_dir = self.codex_home.join("generated_images").join(&thread_id);
        let (notifications, route) = self.server.register(&thread_id);
        let watch = notifications.clone();
        // Checked again after setup: a call that answered while the thread
        // was opening must not start a turn, which would spend the account's
        // image allowance for nothing.
        if self.abandoned.load(Ordering::SeqCst) || Instant::now() >= self.deadline {
            remove(&thread_dir, |path| std::fs::remove_dir_all(path));
            return Err(ImageWireError::Timeout);
        }
        if let SessionEvents::CodexAppServer {
            notifications: slot,
            ..
        } = &mut launched.events
        {
            *slot = Some(notifications);
        }
        let content = MessageContent {
            text: self.turn.text.clone(),
            images: self
                .turn
                .images
                .iter()
                .map(|image| MessageImage {
                    mime_type: image.media_type.clone(),
                    bytes: image.bytes.clone(),
                })
                .collect(),
        };
        if let Err(error) = launched.control.open_turn(&content) {
            // A failed start may still have been accepted (a lost response),
            // so the thread is followed like any turn that ended badly.
            self.release(thread_dir, &[], Some((watch, route)));
            return Err(self.failed("could not start a Codex image turn", error));
        }
        // Whatever the setup took comes out of the fold's time: a fold with
        // none left interrupts the turn at once.
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        let turn = await_one_shot_with(launched, remaining, OneShotAllow::ImageGeneration);
        let unsettled = turn.reply.is_err().then_some((watch, route));
        self.release(thread_dir, &turn.images, unsettled);
        match turn.reply {
            Ok(reply) => Ok(Turn::Answered {
                reply: Box::new(reply),
                images: turn.images,
            }),
            Err(OneShotError::Timeout) => Err(ImageWireError::Timeout),
            Err(_) if self.server.is_closed() => Err(ImageWireError::Transport(
                "the Codex app-server closed during an image turn".to_string(),
            )),
            Err(error) => Ok(Turn::Missed(error.to_string())),
        }
    }

    /// Delete what Codex saved for a thread: every image item's file and the
    /// thread's directory under `generated_images`. A turn that did not end
    /// cleanly (`unsettled`) is followed to its end first, on a thread of its
    /// own (see [`DRAIN_QUIET`]). Best effort: a copy that cannot be removed
    /// is logged, never an error.
    fn release(
        &self,
        thread_dir: PathBuf,
        images: &[ImageGenerationItem],
        unsettled: Option<(Receiver<Value>, ThreadRoute)>,
    ) {
        for saved in images
            .iter()
            .filter_map(|image| image.saved_path.as_deref())
        {
            remove(Path::new(saved), |path| std::fs::remove_file(path));
        }
        remove(&thread_dir, |path| std::fs::remove_dir_all(path));
        if let Some((watch, route)) = unsettled {
            let client = self.server.client();
            std::thread::spawn(move || drain(&client, watch, route, &thread_dir));
        }
    }
}

/// Follow a thread whose turn the call let go until that turn ends, removing
/// each image that completes on it, then the thread's directory.
fn drain(client: &AppServerClient, watch: Receiver<Value>, route: ThreadRoute, thread_dir: &Path) {
    let until = Instant::now() + DRAIN_LIMIT;
    loop {
        let left = until.saturating_duration_since(Instant::now());
        let message = match watch.recv_timeout(left.min(DRAIN_QUIET)) {
            Ok(message) => message,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                log::warn!(
                    "codex image: thread {} said nothing for {:?} before its turn ended; an image it writes after this stays in {}",
                    route.thread_id(),
                    left.min(DRAIN_QUIET),
                    thread_dir.display()
                );
                break;
            }
        };
        match Notification::parse(&message) {
            Notification::ItemCompleted { item, .. } => {
                if let Some(saved) =
                    ImageGenerationItem::parse(item).and_then(|image| image.saved_path)
                {
                    remove(Path::new(&saved), |path| std::fs::remove_file(path));
                }
            }
            Notification::TurnCompleted { .. } | Notification::TurnAborted { .. } => break,
            Notification::ServerRequest { id, .. } => {
                let _ = client.respond_error(id, -32601, "this image call has ended");
            }
            _ => {}
        }
    }
    drop(route);
    remove(thread_dir, |path| std::fs::remove_dir_all(path));
}

fn remove(path: &Path, remove: impl FnOnce(&Path) -> std::io::Result<()>) {
    if let Err(error) = remove(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            log::warn!("codex image: could not remove {}: {error}", path.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use cairn_platform::services::process::{ChildProcess, SpawnConfig};
    use crossbeam_channel::Sender;
    use serde_json::json;

    use crate::codex::app_server::AppServerClient;
    use crate::codex::thread_router::Scoped;
    use crate::interface::image::{ImageCall, ImageInput, ImageRequest, Reference};
    use crate::interface::session::{Cli, CodexModels, SessionError};
    use crate::interface::{Catalog, CatalogProvider};
    use crate::types::catalog::{descriptor, ProviderModelCatalog, VerbKind};

    const PNG_2X3: &str = "iVBORw0KGgoAAAANSUhEUgAAAAIAAAADCAIAAAA2iEnWAAAAFElEQVR4nGM8ISfHwMDAxMDAgKAAFEoBCqHkTQMAAAAASUVORK5CYII=";

    /// A shared server's threads start no process of their own.
    struct NoProcessHost;

    impl SessionHost for NoProcessHost {
        fn name(&self) -> &str {
            "test-host"
        }
        fn resolve_cli(&self, _: Cli) -> Result<String, SessionError> {
            unreachable!("a shared thread resolves no CLI")
        }
        fn spawn(&self, _: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
            unreachable!("a shared thread spawns nothing")
        }
        fn place_file(&self, _: &str, _: &str, _: &[u8]) -> Result<String, String> {
            unreachable!("a shared thread places no files")
        }
        fn prepare_codex_home(&self, _: CodexModels<'_>) -> Result<String, SessionError> {
            unreachable!("a shared thread prepares no home")
        }
    }

    fn notify(thread: &str, method: &str, params: Value) -> Value {
        let mut params = params;
        params["threadId"] = json!(thread);
        json!({"method": method, "params": params})
    }

    /// What the app-server says for one thread's turn after `item/started`.
    /// `t1` answers without calling the tool; every other thread draws one
    /// image, saved under `home`, its revised prompt naming its thread.
    fn rest_of_turn(thread: &str, home: &Path) -> Vec<Value> {
        let counts = json!({"totalTokens": 1040, "inputTokens": 1000,
            "cachedInputTokens": 800, "outputTokens": 40, "reasoningOutputTokens": 0});
        let usage = json!({"tokenUsage": {"total": counts, "last": counts}});
        let done = notify(
            thread,
            "turn/completed",
            json!({"turn": {"id": format!("turn-{thread}"), "status": "completed"}}),
        );
        if thread == "t1" {
            return vec![
                notify(
                    thread,
                    "item/completed",
                    json!({"item": {"id": "m", "type": "agentMessage", "text": "I will not."}}),
                ),
                notify(thread, "thread/tokenUsage/updated", usage),
                done,
            ];
        }
        let saved = home
            .join("generated_images")
            .join(thread)
            .join(format!("exec-{thread}.png"));
        vec![
            notify(
                thread,
                "item/started",
                json!({"item": {"id": format!("exec-{thread}"), "type": "imageGeneration", "status": "in_progress", "result": ""}}),
            ),
            notify(
                thread,
                "item/completed",
                json!({"item": {"id": format!("exec-{thread}"), "type": "imageGeneration",
                "status": "completed", "revisedPrompt": thread, "result": PNG_2X3, "failure": null,
                "savedPath": saved.to_string_lossy()}}),
            ),
            notify(
                thread,
                "item/completed",
                json!({"item": {"id": "m", "type": "agentMessage", "text": "DONE"}}),
            ),
            notify(thread, "thread/tokenUsage/updated", usage),
            done,
        ]
    }

    struct Script {
        server: Sender<Value>,
        opened: u32,
        started: Vec<String>,
        home: std::path::PathBuf,
    }

    impl Script {
        fn answer(&mut self, request: &Value) -> Value {
            match request["method"].as_str().unwrap() {
                "thread/start" => {
                    self.opened += 1;
                    json!({"result": {"thread": {"id": format!("t{}", self.opened)}}})
                }
                "turn/start" => {
                    let thread = request["params"]["threadId"].as_str().unwrap().to_string();
                    self.started.push(thread.clone());
                    match self.started.len() {
                        // Hold the first turn until the second has started,
                        // then interleave both threads' messages.
                        1 => {}
                        2 => {
                            let first = rest_of_turn(&self.started[0], &self.home);
                            let second = rest_of_turn(&self.started[1], &self.home);
                            let longest = first.len().max(second.len());
                            for index in 0..longest {
                                for turn in [&first, &second] {
                                    if let Some(message) = turn.get(index) {
                                        self.server.send(message.clone()).unwrap();
                                    }
                                }
                            }
                        }
                        _ => {
                            for message in rest_of_turn(&thread, &self.home) {
                                self.server.send(message).unwrap();
                            }
                        }
                    }
                    json!({"result": {"turn": {"id": format!("turn-{thread}")}}})
                }
                _ => json!({"result": {}}),
            }
        }
    }

    #[test]
    fn two_images_draw_on_two_routed_threads_and_a_miss_is_retried() {
        let home = tempfile::tempdir().unwrap();
        for thread in ["t2", "t3"] {
            let dir = home.path().join("generated_images").join(thread);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(format!("exec-{thread}.png")), b"png").unwrap();
        }
        let requests = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(None::<Script>));
        let answering = script.clone();
        let (client, server) = AppServerClient::for_test_with_notifications(
            move |request| Ok(answering.lock().unwrap().as_mut().unwrap().answer(request)),
            requests.clone(),
        );
        *script.lock().unwrap() = Some(Script {
            server,
            opened: 0,
            started: Vec::new(),
            home: home.path().to_path_buf(),
        });
        let router = ThreadRouter::start(Arc::new(client), |_, _| Scoped::Passed, || {});
        let transport = CodexImages {
            session_host: Arc::new(NoProcessHost),
            server: router.clone(),
            codex_home: home.path().to_path_buf(),
            http: HttpImages,
        };
        let route = ImageRoute::Codex(CodexImageRoute::chatgpt(Some("gpt-worker".into())));
        let capabilities = ImageCapabilities::codex_image_gen();
        let mut request = ImageRequest::new("a lighthouse");
        request.count = 2;
        request.references = vec![Reference {
            image: ImageInput {
                bytes: crate::image_wire::decode_base64(PNG_2X3).unwrap(),
                media_type: "image/png".into(),
            },
            label: Some("the style".into()),
        }];

        let result = crate::image::image(
            &transport,
            ImageCall {
                route: &route,
                model: "gpt-image-2",
                capabilities: &capabilities,
                request: &request,
                timeout: Duration::from_secs(10),
            },
        )
        .unwrap();

        // Each thread's messages reached only its own fold: t1's worker saw
        // no image, so it was retried as t3, and t2 and t3 each drew theirs.
        let drawn: BTreeSet<_> = result
            .images
            .iter()
            .map(|image| image.revised_prompt.clone().unwrap())
            .collect();
        assert_eq!(drawn, BTreeSet::from(["t2".to_string(), "t3".to_string()]));
        assert_eq!(result.images[0].media_type, "image/png");
        // Three worker turns' tokens: 3 x 1000 prompt, 3 x 40 output.
        assert_eq!(result.usage.prompt_tokens(), Some(3_000));

        let requests = requests.lock().unwrap();
        let starts: Vec<&Value> = requests
            .iter()
            .filter(|request| request["method"] == "thread/start")
            .collect();
        assert_eq!(starts.len(), 3);
        for start in &starts {
            let params = &start["params"];
            assert_eq!(params["model"], "gpt-worker");
            assert_eq!(params["baseInstructions"], WORKER_INSTRUCTIONS);
            assert_eq!(params["ephemeral"], true);
            assert_eq!(params["environments"], json!([]));
            assert_eq!(params["sandbox"], "read-only");
            assert_eq!(
                params["config"],
                json!({
                    "web_search": "disabled",
                    "tools.update_plan.enabled": false,
                    "tools.experimental_request_user_input.enabled": false,
                    "features.image_generation": true,
                    "model_reasoning_effort": "low",
                })
            );
        }
        let turn = requests
            .iter()
            .find(|request| request["method"] == "turn/start")
            .unwrap();
        let input = turn["params"]["input"].as_array().unwrap();
        assert_eq!(input.len(), 2);
        assert_eq!(
            input[0],
            json!({"type": "text", "text": turn_text(&result.provenance.prompt_sent)})
        );
        assert_eq!(
            result.provenance.prompt_sent,
            "a lighthouse\n\nImage 1: the style"
        );
        assert_eq!(input[1]["type"], "image");
        assert_eq!(input[1]["url"], format!("data:image/png;base64,{PNG_2X3}"));

        // Codex's own copies are gone, with their thread directories.
        let images = home.path().join("generated_images");
        assert_eq!(std::fs::read_dir(&images).unwrap().count(), 0);
        // Every route was dropped with its turn.
        for thread in ["t1", "t2", "t3"] {
            assert!(!router.is_routed(thread), "{thread}");
        }
    }

    /// The row serves `image` alone: no session or completion lands on it,
    /// and an image request is admitted on it.
    #[test]
    fn the_catalog_row_serves_only_image_over_codex() {
        let row = discovered_model();
        assert_eq!(row.id, "gpt-image-2");
        let serves = row.serves.clone().unwrap();
        assert_eq!(serves.len(), 1);
        assert_eq!(serves[0].wire, Wire::CodexAppServer);
        assert_eq!(
            serves[0].verb.image(),
            Some(&ImageCapabilities::codex_image_gen())
        );
        let catalog = Catalog {
            providers: vec![CatalogProvider {
                descriptor: descriptor("codex").expect("Codex ships").clone(),
                installed: true,
                models: Some(ProviderModelCatalog {
                    backend: "codex".into(),
                    models: vec![row],
                    options: Vec::new(),
                    refreshed_at: Some(1),
                    error: None,
                }),
                tiers: Vec::new(),
                serves: Vec::new(),
                unable: Vec::new(),
            }],
        };
        for verb in [VerbKind::Session, VerbKind::Complete] {
            assert!(catalog.offers_serving(verb).is_empty(), "{verb:?}");
        }
        let (offer, capabilities) = catalog
            .image_offer(None, &ImageRequest::new("a lighthouse"))
            .unwrap();
        assert_eq!(
            (offer.provider.as_str(), offer.model.as_str()),
            ("codex", "gpt-image-2")
        );
        assert_eq!(capabilities, ImageCapabilities::codex_image_gen());
    }

    #[test]
    fn a_server_that_closes_mid_turn_is_a_transport_failure() {
        let (client, server) = AppServerClient::for_test_with_notifications(
            |request| {
                Ok(match request["method"].as_str().unwrap() {
                    "thread/start" => json!({"result": {"thread": {"id": "t1"}}}),
                    _ => json!({"result": {"turn": {"id": "turn-1"}}}),
                })
            },
            Arc::new(Mutex::new(Vec::new())),
        );
        let router = ThreadRouter::start(Arc::new(client), |_, _| Scoped::Passed, || {});
        let transport = CodexImages {
            session_host: Arc::new(NoProcessHost),
            server: router,
            codex_home: tempfile::tempdir().unwrap().path().to_path_buf(),
            http: HttpImages,
        };
        // The server dies as soon as the turn has started.
        let closer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            drop(server);
        });
        let route = ImageRoute::Codex(CodexImageRoute::chatgpt(None));
        let error = transport
            .post(ImagePost {
                route: &route,
                body: ImageBody::Codex(CodexImageTurn {
                    instructions: WORKER_INSTRUCTIONS.into(),
                    images: Vec::new(),
                    text: turn_text("a fox"),
                    count: 1,
                    effort: "low".into(),
                }),
                timeout: Duration::from_secs(10),
            })
            .unwrap_err();
        closer.join().unwrap();
        assert_eq!(
            error,
            ImageWireError::Transport("the Codex app-server closed during an image turn".into())
        );
    }

    type OnTurn = Box<dyn FnMut(&str, &mut Option<Sender<Value>>) + Send>;

    /// A server that answers `thread/start` (threads `t1`, `t2`, ...) after
    /// `open_delay` and `turn/start` after `turn_delay`, then lets `on_turn`
    /// speak for that thread, or close the server by taking its sender.
    fn scripted(open_delay: Duration, turn_delay: Duration, on_turn: OnTurn) -> Arc<ThreadRouter> {
        scripted_with(open_delay, turn_delay, false, on_turn).0
    }

    /// [`scripted`], optionally losing every `turn/start` response after the
    /// turn was accepted, and with every request the server was sent.
    fn scripted_with(
        open_delay: Duration,
        turn_delay: Duration,
        lose_turn_responses: bool,
        mut on_turn: OnTurn,
    ) -> (Arc<ThreadRouter>, Arc<Mutex<Vec<Value>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let sender: Arc<Mutex<Option<Sender<Value>>>> = Arc::new(Mutex::new(None));
        let held = sender.clone();
        let mut opened = 0;
        let (client, server) = AppServerClient::for_test_with_notifications(
            move |request| {
                Ok(match request["method"].as_str().unwrap() {
                    "thread/start" => {
                        std::thread::sleep(open_delay);
                        opened += 1;
                        json!({"result": {"thread": {"id": format!("t{opened}")}}})
                    }
                    "turn/start" => {
                        std::thread::sleep(turn_delay);
                        let thread = request["params"]["threadId"].as_str().unwrap().to_string();
                        on_turn(&thread, &mut held.lock().unwrap());
                        if lose_turn_responses {
                            return Err("the turn/start response was lost".to_string());
                        }
                        json!({"result": {"turn": {"id": format!("turn-{thread}")}}})
                    }
                    _ => json!({"result": {}}),
                })
            },
            requests.clone(),
        );
        *sender.lock().unwrap() = Some(server);
        let router = ThreadRouter::start(Arc::new(client), |_, _| Scoped::Passed, || {});
        (router, requests)
    }

    /// One image on `router`, with Codex's home at `home`, and how long the
    /// call took.
    fn draw_one(
        router: Arc<ThreadRouter>,
        home: &Path,
        timeout: Duration,
    ) -> (Result<ImageReply, ImageWireError>, Duration) {
        let transport = CodexImages {
            session_host: Arc::new(NoProcessHost),
            server: router,
            codex_home: home.to_path_buf(),
            http: HttpImages,
        };
        let route = ImageRoute::Codex(CodexImageRoute::chatgpt(None));
        let started = Instant::now();
        let result = transport.post(ImagePost {
            route: &route,
            body: ImageBody::Codex(CodexImageTurn {
                instructions: WORKER_INSTRUCTIONS.into(),
                images: Vec::new(),
                text: turn_text("a fox"),
                count: 1,
                effort: "low".into(),
            }),
            timeout,
        });
        (result, started.elapsed())
    }

    /// Codex's saved copy of `thread`'s image, as `rest_of_turn` names it.
    fn saved(home: &Path, thread: &str) -> std::path::PathBuf {
        let dir = home.join("generated_images").join(thread);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join(format!("exec-{thread}.png"));
        std::fs::write(&file, b"png").unwrap();
        file
    }

    fn image_done(thread: &str, saved: &Path) -> Value {
        notify(
            thread,
            "item/completed",
            json!({"item": {"id": format!("exec-{thread}"), "type": "imageGeneration",
                "status": "completed", "revisedPrompt": thread, "result": PNG_2X3,
                "failure": null, "savedPath": saved.to_string_lossy()}}),
        )
    }

    fn forbidden_tool(thread: &str) -> Value {
        notify(
            thread,
            "item/started",
            json!({"item": {"id": "c", "type": "commandExecution"}}),
        )
    }

    /// Whether `path` is gone within a few seconds: a worker the call has
    /// already answered without cleans up on its own thread.
    fn eventually_gone(path: &Path) -> bool {
        let until = Instant::now() + Duration::from_secs(5);
        while Instant::now() < until {
            if !path.exists() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        false
    }

    /// The deadline bounds the whole call: an app-server slow to open the
    /// thread, or to start its turn, cannot hold it past its timeout, and
    /// running out of time is a timeout, not a transport failure.
    #[test]
    fn the_deadline_bounds_thread_and_turn_setup() {
        let home = tempfile::tempdir().unwrap();
        for (open, turn) in [
            (Duration::from_secs(3), Duration::ZERO),
            (Duration::ZERO, Duration::from_secs(3)),
        ] {
            let router = scripted(open, turn, Box::new(|_, _| {}));
            let (result, took) = draw_one(router, home.path(), Duration::from_millis(300));
            assert_eq!(result.unwrap_err(), ImageWireError::Timeout);
            assert!(took < Duration::from_secs(2), "the call took {took:?}");
        }
    }

    #[test]
    fn an_image_before_a_silent_deadline_leaves_no_copy() {
        let home = tempfile::tempdir().unwrap();
        let copy = saved(home.path(), "t1");
        let image = image_done("t1", &copy);
        let router = scripted(
            Duration::ZERO,
            Duration::ZERO,
            Box::new(move |_, server| {
                server.as_ref().unwrap().send(image.clone()).unwrap();
            }),
        );
        let (result, _) = draw_one(router, home.path(), Duration::from_millis(500));
        assert_eq!(result.unwrap_err(), ImageWireError::Timeout);
        assert!(
            eventually_gone(copy.parent().unwrap()),
            "t1's images remain"
        );
    }

    #[test]
    fn an_image_before_the_server_closes_leaves_no_copy() {
        let home = tempfile::tempdir().unwrap();
        let copy = saved(home.path(), "t1");
        let image = image_done("t1", &copy);
        let router = scripted(
            Duration::ZERO,
            Duration::ZERO,
            Box::new(move |_, server| {
                server.take().unwrap().send(image.clone()).unwrap();
            }),
        );
        let (result, _) = draw_one(router, home.path(), Duration::from_secs(5));
        assert_eq!(
            result.unwrap_err(),
            ImageWireError::Transport("the Codex app-server closed during an image turn".into())
        );
        assert!(
            eventually_gone(copy.parent().unwrap()),
            "t1's images remain"
        );
    }

    /// A forbidden tool after an image fails that turn; the retry draws the
    /// image, and neither thread leaves a copy behind.
    #[test]
    fn an_image_before_a_forbidden_tool_is_retried_and_leaves_no_copy() {
        let home = tempfile::tempdir().unwrap();
        let first = saved(home.path(), "t1");
        let second = saved(home.path(), "t2");
        let (first_image, rest) = (image_done("t1", &first), rest_of_turn("t2", home.path()));
        let router = scripted(
            Duration::ZERO,
            Duration::ZERO,
            Box::new(move |thread, server| {
                let server = server.as_ref().unwrap();
                if thread == "t1" {
                    server.send(first_image.clone()).unwrap();
                    server.send(forbidden_tool("t1")).unwrap();
                } else {
                    for message in &rest {
                        server.send(message.clone()).unwrap();
                    }
                }
            }),
        );
        let (result, _) = draw_one(router, home.path(), Duration::from_secs(5));
        let ImageReply::Codex(reply) = result.unwrap() else {
            panic!("a Codex reply");
        };
        let drawn = reply.draws[0].image.as_ref().unwrap();
        assert_eq!(drawn.revised_prompt.as_deref(), Some("t2"));
        assert!(
            eventually_gone(first.parent().unwrap()),
            "t1's images remain"
        );
        assert!(
            eventually_gone(second.parent().unwrap()),
            "t2's images remain"
        );
    }

    /// An image the interrupted tool finishes well after its turn was let go
    /// is removed when it completes, and the thread's directory once the turn
    /// has really ended.
    #[test]
    fn an_image_that_lands_after_its_turn_was_let_go_is_removed() {
        let home = tempfile::tempdir().unwrap();
        saved(home.path(), "t2");
        let late = home.path().join("generated_images").join("t1");
        let (landing, rest) = (late.clone(), rest_of_turn("t2", home.path()));
        let router = scripted(
            Duration::ZERO,
            Duration::ZERO,
            Box::new(move |thread, server| {
                let server = server.as_ref().unwrap();
                if thread == "t1" {
                    server.send(forbidden_tool("t1")).unwrap();
                    let (landing, server) = (landing.clone(), server.clone());
                    std::thread::spawn(move || {
                        // Long after the worker has moved on to its retry.
                        std::thread::sleep(Duration::from_millis(600));
                        std::fs::create_dir_all(&landing).unwrap();
                        let file = landing.join("exec-t1.png");
                        std::fs::write(&file, b"png").unwrap();
                        server.send(image_done("t1", &file)).unwrap();
                        std::thread::sleep(Duration::from_millis(200));
                        server
                            .send(notify(
                                "t1",
                                "turn/aborted",
                                json!({"reason": "interrupted"}),
                            ))
                            .unwrap();
                    });
                } else {
                    for message in &rest {
                        server.send(message.clone()).unwrap();
                    }
                }
            }),
        );
        let (result, _) = draw_one(router, home.path(), Duration::from_secs(5));
        assert!(result.is_ok(), "{result:?}");
        std::thread::sleep(Duration::from_millis(700));
        assert!(
            !late.join("exec-t1.png").exists(),
            "the late image was removed as it completed"
        );
        assert!(eventually_gone(&late), "t1's directory remains");
    }

    /// A call that has answered starts no turn: a worker whose thread opened
    /// only after the deadline removes it unused.
    #[test]
    fn no_turn_starts_after_the_call_has_answered() {
        let home = tempfile::tempdir().unwrap();
        let (router, requests) = scripted_with(
            Duration::from_secs(1),
            Duration::ZERO,
            false,
            Box::new(|_, _| {}),
        );
        let (result, _) = draw_one(router, home.path(), Duration::from_millis(300));
        assert_eq!(result.unwrap_err(), ImageWireError::Timeout);
        // Past the slow thread/start, when the worker would have gone on.
        std::thread::sleep(Duration::from_millis(1500));
        let methods: Vec<String> = requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|request| request["method"].as_str().map(str::to_string))
            .collect();
        assert!(methods.contains(&"thread/start".to_string()), "{methods:?}");
        assert!(!methods.contains(&"turn/start".to_string()), "{methods:?}");
    }

    /// A turn/start whose response is lost may still have been accepted: the
    /// thread is followed, and the image it draws leaves no copy.
    #[test]
    fn an_accepted_turn_whose_start_response_is_lost_leaves_no_copy() {
        let home = tempfile::tempdir().unwrap();
        let copy = saved(home.path(), "t1");
        let rest = vec![
            image_done("t1", &copy),
            notify(
                "t1",
                "turn/completed",
                json!({"turn": {"id": "turn-t1", "status": "completed"}}),
            ),
        ];
        let (router, _) = scripted_with(
            Duration::ZERO,
            Duration::ZERO,
            true,
            Box::new(move |_, server| {
                for message in &rest {
                    server.as_ref().unwrap().send(message.clone()).unwrap();
                }
            }),
        );
        let (result, _) = draw_one(router, home.path(), Duration::from_secs(5));
        assert!(
            matches!(&result, Err(ImageWireError::Transport(text)) if text.contains("could not start")),
            "{result:?}"
        );
        assert!(
            eventually_gone(copy.parent().unwrap()),
            "t1's images remain"
        );
    }
}
