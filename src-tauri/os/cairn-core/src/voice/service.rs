use super::{
    current_target, model_catalog, ComponentInstaller, ComponentState, DictationMode, EngineState,
    InstallEvent, ModelPurpose, ProtocolCommand, ProtocolEvent, Transcript, VoiceError, VoiceEvent,
    VoiceModel, VoiceProcess, VoiceResult, VoiceStatus,
};
use crate::services::EventEmitter;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, Semaphore};
use uuid::Uuid;

const RELEASE_BASE: &str = "https://github.com/bleugreenlab/cairn/releases/download";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
pub const VOICE_EVENT_NAME: &str = "voice-event";

/// The engine's single live session, as far as this service knows.
///
/// The distinction that earns the third variant is between a session that is
/// merely open and one that is being closed. An open session may be closed and
/// replaced by a new dictation — a caller that lost its handle must not be able
/// to wedge dictation for the life of the process. A closing one may not be
/// treated as absent: the engine has not yet said it is done with it, so anyone
/// admitted on the strength of a cleared handle would meet an engine that is
/// still busy. Only `Idle` means the engine is free.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum StreamState {
    #[default]
    Idle,
    Open(String),
    Closing(String),
}

async fn engine_component_state(
    installer: &ComponentInstaller,
    components: &[super::ManagedComponent],
) -> ComponentState {
    let Some(component) = components
        .iter()
        .find(|component| component.kind == "engine")
    else {
        return ComponentState::Missing;
    };
    let Some(target) = current_target() else {
        return ComponentState::Incompatible;
    };
    if !installer.engine_is_current(component, target) {
        return ComponentState::Incompatible;
    }
    match installer.verify(component).await {
        Ok(()) => ComponentState::Installed,
        Err(_) => ComponentState::Corrupt,
    }
}

impl StreamState {
    fn is_idle(&self) -> bool {
        matches!(self, StreamState::Idle)
    }

    /// Whether this state is about `stream_id`, in either phase.
    fn names(&self, stream_id: &str) -> bool {
        match self {
            StreamState::Open(id) | StreamState::Closing(id) => id == stream_id,
            StreamState::Idle => false,
        }
    }
}

struct VoiceServiceInner {
    installer: ComponentInstaller,
    config_dir: PathBuf,
    app_version: String,
    engine_override: Option<PathBuf>,
    enabled: AtomicBool,
    preferred_file_model: Mutex<VoiceModel>,
    preferred_dictation_model: Mutex<VoiceModel>,
    dictation_mode: Mutex<DictationMode>,
    input_device_id: Mutex<Option<String>>,
    loaded_model: Mutex<Option<VoiceModel>>,
    /// What the engine's single live session is doing, as far as this service
    /// knows. See [`StreamState`].
    stream_state: Mutex<StreamState>,
    shutting_down: AtomicBool,
    process: Mutex<Option<VoiceProcess>>,
    file_transcription_cancellations: Mutex<HashMap<String, Arc<Notify>>>,
    stream_feed_admission: Arc<Semaphore>,
    engine_state: Mutex<EngineState>,
    restart_attempts: AtomicU32,
    emitter: Arc<dyn EventEmitter>,
}

/// Cloneable command surface. The runner retains [`VoiceService`] solely to
/// make ownership and shutdown ordering explicit; transports receive this handle.
#[derive(Clone)]
pub struct VoiceServiceHandle(Arc<VoiceServiceInner>);

pub struct VoiceService {
    handle: VoiceServiceHandle,
}

impl VoiceService {
    /// `build_identity` is the runner's own `/api/health` build id; it decides
    /// whether an engine handed off by a self-rebuild belongs to this app.
    pub fn new(
        data_dir: PathBuf,
        app_version: impl Into<String>,
        build_identity: impl Into<String>,
        emitter: Arc<dyn EventEmitter>,
    ) -> Self {
        // This escape hatch exists only in development/test builds. Production
        // always executes a checksum-pinned managed release artifact.
        let engine_override = cfg!(debug_assertions)
            .then(|| std::env::var_os("CAIRN_VOICE_ENGINE_PATH").map(PathBuf::from))
            .flatten();
        Self::with_engine(
            data_dir,
            app_version,
            build_identity,
            emitter,
            engine_override,
        )
    }

    /// The same service with its engine binary named directly.
    ///
    /// A test drives a scripted engine through this rather than through the
    /// environment variable, which is shared by every test in the binary and so
    /// cannot describe one service's engine without describing them all.
    fn with_engine(
        data_dir: PathBuf,
        app_version: impl Into<String>,
        build_identity: impl Into<String>,
        emitter: Arc<dyn EventEmitter>,
        engine_override: Option<PathBuf>,
    ) -> Self {
        let app_version = app_version.into();
        let preferences = crate::config::settings::load_voice_preferences(&data_dir);
        let install_emitter = emitter.clone();
        let installer = ComponentInstaller::new(
            data_dir.clone(),
            RELEASE_BASE,
            app_version.clone(),
            build_identity,
            move |event| {
                let event = match event {
                    InstallEvent::Started {
                        operation_id,
                        component,
                        total_bytes,
                    } => VoiceEvent::DownloadStarted {
                        operation_id,
                        component,
                        total_bytes,
                    },
                    InstallEvent::Progress {
                        operation_id,
                        component,
                        downloaded_bytes,
                        total_bytes,
                    } => VoiceEvent::DownloadProgress {
                        operation_id,
                        component,
                        downloaded_bytes,
                        total_bytes,
                    },
                    InstallEvent::Finished {
                        operation_id,
                        component,
                    } => VoiceEvent::DownloadFinished {
                        operation_id,
                        component,
                    },
                };
                emit_voice_event(install_emitter.as_ref(), event);
            },
        );
        Self {
            handle: VoiceServiceHandle(Arc::new(VoiceServiceInner {
                installer,
                config_dir: data_dir,
                app_version,
                engine_override,
                enabled: AtomicBool::new(preferences.enabled),
                preferred_file_model: Mutex::new(preferences.preferred_file_model),
                preferred_dictation_model: Mutex::new(preferences.preferred_dictation_model),
                dictation_mode: Mutex::new(preferences.dictation_mode),
                input_device_id: Mutex::new(preferences.input_device_id),
                loaded_model: Mutex::new(None),
                stream_state: Mutex::new(StreamState::Idle),
                shutting_down: AtomicBool::new(false),
                process: Mutex::new(None),
                file_transcription_cancellations: Mutex::new(HashMap::new()),
                stream_feed_admission: Arc::new(Semaphore::new(8)),
                engine_state: Mutex::new(EngineState::Stopped),
                restart_attempts: AtomicU32::new(0),
                emitter,
            })),
        }
    }

    pub fn handle(&self) -> VoiceServiceHandle {
        self.handle.clone()
    }

    /// Refresh a version-pinned managed engine, or adopt one a self-rebuild
    /// handed off to this runner, then warm the dictation model, without
    /// delaying host startup. Failure is deliberately non-fatal: the
    /// request path retries the same operations when voice is next used.
    ///
    /// The warm-up follows the refresh rather than running beside it, so it
    /// never loads a model into an engine that is about to be replaced.
    pub fn start_background_refresh(&self) {
        if !self.handle.0.enabled.load(Ordering::Acquire) {
            return;
        }
        let voice = self.handle.clone();
        tokio::spawn(async move {
            voice.refresh_engine().await;
            voice.warm_dictation_model().await;
        });
    }

    pub fn begin_shutdown(&self) {
        self.handle.0.shutting_down.store(true, Ordering::Release);
    }

    pub async fn shutdown(self) {
        self.begin_shutdown();
        let _ = self.handle.stop_process().await;
    }
}

impl VoiceServiceHandle {
    async fn refresh_engine(&self) {
        if self.0.engine_override.is_some() {
            return;
        }
        let Some(target) = current_target() else {
            return;
        };
        let stale = match self.0.installer.manifest().await {
            Ok(manifest) => manifest
                .components
                .iter()
                .find(|component| component.kind == "engine")
                .is_some_and(|component| !self.0.installer.engine_is_current(component, target)),
            Err(error) => {
                log::warn!("Could not inspect voice engine during startup: {error}");
                return;
            }
        };
        if stale || self.0.installer.local_engine_pending().await {
            if let Err(error) = self.0.installer.install_engine(target).await {
                log::warn!("Could not refresh voice engine during startup: {error}");
            }
        }
    }

    /// Warm the dictation model in the background. See
    /// [`Self::warm_dictation_model`].
    pub fn prewarm_dictation(&self) {
        let voice = self.clone();
        tokio::spawn(async move {
            voice.warm_dictation_model().await;
        });
    }

    /// Load the preferred dictation model now, so the first dictation opens on
    /// a warm engine instead of paying for the load out of the front of the
    /// user's sentence. A cold load is seconds — the model file is hashed,
    /// read, and on Metal its shaders compiled — while a warm stream opens in a
    /// fraction of one.
    ///
    /// The process lock is the single flight: a dictation pressed mid-warm-up
    /// waits on this very load rather than starting another, and finds the
    /// model it needs already in place. A warm-up is opportunistic, so it
    /// never waits for the engine. It claims the process lock without
    /// queueing and declines if anyone holds it, then checks for a live
    /// session or a file job *under* that claim, so work admitted a moment
    /// before is seen and declined for, and work admitted a moment after
    /// waits on the load like any other caller. It never downloads either: an
    /// engine that needs replacing is the refresh's job.
    ///
    /// Returns whether the model is now loaded.
    async fn warm_dictation_model(&self) -> bool {
        if self.0.shutting_down.load(Ordering::Acquire) || !self.0.enabled.load(Ordering::Acquire) {
            return false;
        }
        // Read before claiming: it is file I/O, and nothing should hold the
        // engine while it runs.
        if !self
            .can_load_without_installing(self.preferred_dictation_model().await)
            .await
        {
            return false;
        }
        let Ok(guard) = self.0.process.try_lock() else {
            return false;
        };
        {
            // Process, then admission locks: safe because no path holds an
            // admission lock while it waits for the process.
            let cancellations = self.0.file_transcription_cancellations.lock().await;
            if !cancellations.is_empty() || !self.0.stream_state.lock().await.is_idle() {
                return false;
            }
        }
        // Re-read under the claim, so a preference changed while this warm-up
        // was being scheduled loads the model chosen last.
        let model = self.preferred_dictation_model().await;
        let result = self.load_into(guard, model).await.map(drop);
        self.invalidate_on_transport(&result).await;
        if let Err(error) = &result {
            log::warn!("Could not pre-warm the dictation model: {error}");
        }
        result.is_ok()
    }

    /// Whether `model` and a current engine are both already on disk.
    async fn can_load_without_installing(&self, model: VoiceModel) -> bool {
        let Ok(manifest) = self.0.installer.manifest().await else {
            return false;
        };
        if find_component(&manifest.components, model_kind(model)).is_err() {
            return false;
        }
        if self.0.engine_override.is_some() {
            return true;
        }
        let Some(target) = current_target() else {
            return false;
        };
        find_component(&manifest.components, "engine")
            .is_ok_and(|engine| self.0.installer.engine_is_current(engine, target))
    }

    pub async fn set_dictation_mode(&self, mode: DictationMode) -> VoiceResult<VoiceStatus> {
        let mut preferences = self
            .preferences(self.0.enabled.load(Ordering::Acquire))
            .await;
        preferences.dictation_mode = mode;
        self.persist_preferences(preferences)?;
        *self.0.dictation_mode.lock().await = mode;
        self.status().await
    }

    /// Remember which microphone dictation should open. `None` is the system
    /// default; capture itself falls back to the default when the remembered
    /// device is gone, so nothing here has to verify that it still exists.
    pub async fn set_input_device(&self, device_id: Option<String>) -> VoiceResult<VoiceStatus> {
        let mut preferences = self
            .preferences(self.0.enabled.load(Ordering::Acquire))
            .await;
        preferences.input_device_id = device_id.clone();
        self.persist_preferences(preferences)?;
        *self.0.input_device_id.lock().await = device_id;
        self.status().await
    }

    pub async fn preferred_file_model(&self) -> VoiceModel {
        *self.0.preferred_file_model.lock().await
    }

    pub async fn preferred_dictation_model(&self) -> VoiceModel {
        *self.0.preferred_dictation_model.lock().await
    }
    pub fn begin_shutdown(&self) {
        self.0.shutting_down.store(true, Ordering::Release);
    }

    pub async fn status(&self) -> VoiceResult<VoiceStatus> {
        let manifest = self.0.installer.manifest().await?;
        let engine = if let Some(path) = &self.0.engine_override {
            match super::installer::probe_engine(path, &self.0.app_version).await {
                Ok(()) => ComponentState::Installed,
                Err(_) => ComponentState::Incompatible,
            }
        } else {
            engine_component_state(&self.0.installer, &manifest.components).await
        };
        let fast_model =
            component_state(&self.0.installer, &manifest.components, "model:fast").await;
        let accurate_model =
            component_state(&self.0.installer, &manifest.components, "model:accurate").await;
        let realtime_model =
            component_state(&self.0.installer, &manifest.components, "model:realtime").await;
        Ok(VoiceStatus {
            supported: current_target().is_some(),
            enabled: self.0.enabled.load(Ordering::Acquire),
            engine,
            engine_state: self.0.engine_state.lock().await.clone(),
            fast_model,
            accurate_model,
            realtime_model,
            preferred_file_model: *self.0.preferred_file_model.lock().await,
            preferred_dictation_model: *self.0.preferred_dictation_model.lock().await,
            dictation_mode: *self.0.dictation_mode.lock().await,
            input_device_id: self.0.input_device_id.lock().await.clone(),
            detail: None,
        })
    }

    pub async fn enable(&self) -> VoiceResult<VoiceStatus> {
        self.ensure_available()?;
        let target = current_target().ok_or(VoiceError::UnsupportedTarget)?;
        if let Some(path) = &self.0.engine_override {
            super::installer::probe_engine(path, &self.0.app_version).await?;
        } else {
            self.0.installer.install_engine(target).await?;
        }
        self.0.installer.install_model(&model_catalog()[0]).await?;
        self.persist_preferences(self.preferences(true).await)?;
        self.0.enabled.store(true, Ordering::Release);
        self.prewarm_dictation();
        self.status().await
    }

    /// Choose the model one kind of work uses, refusing a model that is not
    /// installed and verified rather than remembering an unusable choice.
    pub async fn set_preferred_model(
        &self,
        purpose: ModelPurpose,
        model: VoiceModel,
    ) -> VoiceResult<VoiceStatus> {
        let manifest = self.0.installer.manifest().await?;
        self.0
            .installer
            .verify(find_component(&manifest.components, model_kind(model))?)
            .await?;
        let mut preferences = self
            .preferences(self.0.enabled.load(Ordering::Acquire))
            .await;
        match purpose {
            ModelPurpose::File => preferences.preferred_file_model = model,
            ModelPurpose::Dictation => preferences.preferred_dictation_model = model,
        }
        self.persist_preferences(preferences)?;
        match purpose {
            ModelPurpose::File => *self.0.preferred_file_model.lock().await = model,
            ModelPurpose::Dictation => {
                let previous =
                    std::mem::replace(&mut *self.0.preferred_dictation_model.lock().await, model);
                if previous != model {
                    self.prewarm_dictation();
                }
            }
        }
        self.status().await
    }

    pub async fn download_model(&self, model: VoiceModel) -> VoiceResult<VoiceStatus> {
        self.ensure_available()?;
        let artifact = model_catalog()
            .iter()
            .find(|entry| entry.model == model)
            .ok_or_else(|| VoiceError::InvalidInput("unknown voice model".into()))?;
        self.0.installer.install_model(artifact).await?;
        self.status().await
    }

    pub async fn disable(&self) -> VoiceResult<VoiceStatus> {
        self.persist_preferences(self.preferences(false).await)?;
        self.0.enabled.store(false, Ordering::Release);
        self.stop_process().await?;
        self.status().await
    }

    pub async fn remove(&self) -> VoiceResult<VoiceStatus> {
        self.disable().await?;
        self.0.installer.remove_managed().await?;
        self.status().await
    }

    pub async fn transcribe_file(
        &self,
        path: &Path,
        model: VoiceModel,
        request_id: Option<String>,
    ) -> VoiceResult<Transcript> {
        let request_id = request_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let cancellation = Arc::new(Notify::new());
        {
            // Declined rather than queued. The engine services one thing at a
            // time, so parking here would hold the process away from a live
            // session's audio for the length of a file — and a dictation UI
            // buffers only seconds of capture before it starts discarding the
            // front of the user's sentence to stay bounded. Both claims are
            // taken under this same pair of locks, in this order, so a file job
            // and a dictation start racing each other cannot both see the
            // engine free.
            let mut cancellations = self.0.file_transcription_cancellations.lock().await;
            if !self.0.stream_state.lock().await.is_idle() {
                return Err(VoiceError::Busy);
            }
            cancellations.insert(request_id.clone(), cancellation.clone());
        }
        let active = Arc::new(AtomicBool::new(false));
        let active_request = active.clone();
        *self.0.engine_state.lock().await = EngineState::Busy;
        emit_voice_event(
            self.0.emitter.as_ref(),
            VoiceEvent::EngineChanged {
                state: EngineState::Busy,
                detail: None,
            },
        );
        let request = async {
            let process = self.process_for(model).await?;
            active_request.store(true, Ordering::Release);
            process
                .request(
                    &ProtocolCommand::TranscribeFile {
                        request_id: request_id.clone(),
                        path: path.to_string_lossy().into_owned(),
                        options: Value::Object(Default::default()),
                    },
                    &request_id,
                )
                .await
        };
        let result = tokio::select! {
            result = request => result,
            () = cancellation.notified() => {
                if active.load(Ordering::Acquire) {
                    self.0.process.lock().await.take();
                    *self.0.loaded_model.lock().await = None;
                }
                Err(VoiceError::Request {
                    code: "cancelled".into(),
                    message: "file transcription was cancelled".into(),
                })
            }
        };
        self.0
            .file_transcription_cancellations
            .lock()
            .await
            .remove(&request_id);
        self.invalidate_on_transport(&result).await;
        let process_restarts = matches!(result, Err(VoiceError::Transport(_)))
            || matches!(result, Err(VoiceError::Request { ref code, .. }) if code == "cancelled");
        let final_state = if process_restarts {
            EngineState::Restarting
        } else {
            EngineState::Ready
        };
        *self.0.engine_state.lock().await = final_state.clone();
        emit_voice_event(
            self.0.emitter.as_ref(),
            VoiceEvent::EngineChanged {
                state: final_state,
                detail: None,
            },
        );
        result
    }

    pub async fn cancel_file_transcription(&self, request_id: &str) {
        if let Some(cancellation) = self
            .0
            .file_transcription_cancellations
            .lock()
            .await
            .get(request_id)
            .cloned()
        {
            cancellation.notify_one();
        }
    }

    pub async fn stream_start(&self) -> VoiceResult<String> {
        let stream_id = Uuid::new_v4().to_string();
        let stale = {
            let cancellations = self.0.file_transcription_cancellations.lock().await;
            if !cancellations.is_empty() {
                return Err(VoiceError::Busy);
            }
            let mut state = self.0.stream_state.lock().await;
            match &*state {
                // A close is in flight. The engine has not said it is done with
                // the previous session, so this one cannot be opened on the
                // assumption that it is — and unlike a stale handle, this
                // resolves on its own in a moment.
                StreamState::Closing(_) => return Err(VoiceError::Busy),
                StreamState::Open(open) => {
                    let stale = open.clone();
                    *state = StreamState::Open(stream_id.clone());
                    Some(stale)
                }
                StreamState::Idle => {
                    *state = StreamState::Open(stream_id.clone());
                    None
                }
            }
        };
        // A session whose owner lost its handle would refuse every dictation
        // that follows with `stream_already_active`, for the life of the
        // process. This service minted that id, so it can always close what the
        // caller can no longer name.
        if let Some(stale) = stale {
            let _ = self.end_stream(stale).await;
        }

        let model = self.preferred_dictation_model().await;
        let request_id = Uuid::new_v4().to_string();
        let result = async {
            let process = self.process_for(model).await?;
            let outcome = process
                .command(
                    &ProtocolCommand::BeginStream {
                        request_id: request_id.clone(),
                        stream_id: stream_id.clone(),
                        // The engine's defaults, for every model. A slower
                        // model needs no slower cadence: a caller keeps one
                        // chunk in flight at a time and an `audioChunk` is only
                        // acknowledged once the partial it made due has been
                        // decoded, so the feed already paces itself against
                        // whatever the model can do. Measured on an M4 Max over
                        // 30 s of speech, raising the interval to 8 s for the
                        // Accurate model cut partials from eight to three and
                        // left the lag where it was — fewer updates, no fresher.
                        options: Value::Object(Default::default()),
                    },
                    &request_id,
                )
                .await;
            drop(process);
            outcome
        }
        .await;
        self.invalidate_on_transport(&result).await;
        if let Err(error) = result {
            self.forget_stream(&stream_id).await;
            if matches!(&error, VoiceError::Request { code, .. } if code == "stream_already_active")
            {
                // The engine is holding a session this service does not know
                // about, so the two have diverged and only a fresh process
                // reconciles them. A recoverable `Request` error must never be
                // able to strand dictation permanently.
                self.recycle_process().await;
            }
            return Err(error);
        }
        Ok(stream_id)
    }

    pub async fn stream_feed(&self, stream_id: String, data: String) -> VoiceResult<()> {
        let _permit = self
            .0
            .stream_feed_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| VoiceError::Backpressure)?;
        let mut guard = self.0.process.lock().await;
        let process = guard.as_mut().ok_or(VoiceError::NotInstalled)?;
        let request_id = Uuid::new_v4().to_string();
        let result = process.feed_audio(request_id, stream_id, data).await;
        drop(guard);
        self.invalidate_on_transport(&result).await;
        result
    }

    pub async fn stream_finish(&self, stream_id: String) -> VoiceResult<Transcript> {
        self.end_stream(stream_id).await
    }

    pub async fn stream_cancel(&self, stream_id: String) -> VoiceResult<()> {
        self.end_stream(stream_id).await.map(|_| ())
    }

    async fn end_stream(&self, stream_id: String) -> VoiceResult<Transcript> {
        // Marked closing rather than cleared. Between here and the engine's
        // answer the session is neither open nor gone, and admitting a file
        // transcription on the strength of a cleared handle would park it on
        // the process mutex this close is about to hold — the exact behaviour
        // the decline exists to prevent.
        {
            let mut state = self.0.stream_state.lock().await;
            if matches!(&*state, StreamState::Open(open) if open == &stream_id) {
                *state = StreamState::Closing(stream_id.clone());
            }
        }

        let request_id = Uuid::new_v4().to_string();
        let mut guard = self.0.process.lock().await;
        let result = match guard.as_mut() {
            Some(process) => {
                process
                    .request(
                        &ProtocolCommand::EndStream {
                            request_id: request_id.clone(),
                            stream_id: stream_id.clone(),
                        },
                        &request_id,
                    )
                    .await
            }
            None => Err(VoiceError::NotInstalled),
        };
        drop(guard);
        self.invalidate_on_transport(&result).await;
        if result.is_err() {
            // The engine may still be holding this session, so merely forgetting
            // it would advertise an engine that is not actually free. Recycling
            // the process is what makes it free in fact; only then is the handle
            // safe to drop, so the two become idle together.
            self.recycle_process().await;
        }
        self.forget_stream(&stream_id).await;
        result
    }

    /// Give up a stream handle, leaving a newer session's handle alone.
    async fn forget_stream(&self, stream_id: &str) {
        let mut state = self.0.stream_state.lock().await;
        if state.names(stream_id) {
            *state = StreamState::Idle;
        }
    }

    /// Drop the engine process so the next request builds a fresh one, and
    /// forget the state that only described the process that is going away.
    async fn recycle_process(&self) {
        self.0.process.lock().await.take();
        *self.0.loaded_model.lock().await = None;
        *self.0.stream_state.lock().await = StreamState::Idle;
        *self.0.engine_state.lock().await = EngineState::Restarting;
    }

    fn ensure_available(&self) -> VoiceResult<()> {
        if self.0.shutting_down.load(Ordering::Acquire) {
            return Err(VoiceError::Transport(
                "voice service is shutting down".into(),
            ));
        }
        Ok(())
    }

    async fn preferences(&self, enabled: bool) -> super::VoicePreferences {
        super::VoicePreferences {
            enabled,
            preferred_file_model: *self.0.preferred_file_model.lock().await,
            preferred_dictation_model: *self.0.preferred_dictation_model.lock().await,
            dictation_mode: *self.0.dictation_mode.lock().await,
            input_device_id: self.0.input_device_id.lock().await.clone(),
        }
    }

    fn persist_preferences(&self, preferences: super::VoicePreferences) -> VoiceResult<()> {
        crate::config::settings::set_voice_preferences(&self.0.config_dir, &preferences)
            .map_err(VoiceError::Install)
    }

    async fn process_for(
        &self,
        model: VoiceModel,
    ) -> VoiceResult<tokio::sync::MappedMutexGuard<'_, VoiceProcess>> {
        let guard = self.0.process.lock().await;
        self.load_into(guard, model).await
    }

    /// Make the process `guard` holds a live engine with `model` loaded.
    ///
    /// The lifecycle is checked here, under the guard, rather than by each
    /// caller before it: `disable` and shutdown clear the flag and then stop
    /// the process under this same lock, so a check made before acquiring it
    /// can be overtaken and would spawn an engine nothing will ever stop.
    async fn load_into<'a>(
        &'a self,
        mut guard: tokio::sync::MutexGuard<'a, Option<VoiceProcess>>,
        model: VoiceModel,
    ) -> VoiceResult<tokio::sync::MappedMutexGuard<'a, VoiceProcess>> {
        self.ensure_available()?;
        if !self.0.enabled.load(Ordering::Acquire) {
            return Err(VoiceError::NotInstalled);
        }
        let process_crashed = match guard.as_ref() {
            Some(process) => !process.is_running().await,
            None => false,
        };
        if process_crashed {
            guard.take();
            *self.0.loaded_model.lock().await = None;
            *self.0.stream_state.lock().await = StreamState::Idle;
            *self.0.engine_state.lock().await = EngineState::Restarting;
            let attempt = self
                .0
                .restart_attempts
                .fetch_add(1, Ordering::AcqRel)
                .min(4);
            tokio::time::sleep(std::time::Duration::from_millis(100 * (1_u64 << attempt))).await;
        }
        // A self-rebuild handed off the engine built alongside this runner. A
        // live process is the engine it supersedes, so it goes now and the
        // spawn below installs and runs the new one. Callers reach here only
        // before starting their work, and the guard keeps anyone else out, so
        // no session or request is in flight on the process being dropped.
        let adopt_handoff =
            self.0.engine_override.is_none() && self.0.installer.local_engine_pending().await;
        if adopt_handoff && guard.is_some() {
            guard.take();
            *self.0.loaded_model.lock().await = None;
        }
        if guard.is_none() {
            let manifest = self.0.installer.manifest().await?;
            let engine_path = if let Some(path) = &self.0.engine_override {
                super::installer::probe_engine(path, &self.0.app_version).await?;
                path.clone()
            } else {
                let target = current_target().ok_or(VoiceError::UnsupportedTarget)?;
                let engine = find_component(&manifest.components, "engine")?;
                let engine = if !adopt_handoff && self.0.installer.engine_is_current(engine, target)
                {
                    engine.clone()
                } else {
                    self.0.installer.install_engine(target).await?
                };
                self.0.installer.verify(&engine).await?;
                engine.path
            };
            let model_component = find_component(&manifest.components, model_kind(model))?;
            self.0.installer.verify(model_component).await?;
            *self.0.engine_state.lock().await = EngineState::Starting;
            let emitter = self.0.emitter.clone();
            let started = async {
                let process =
                    VoiceProcess::spawn(&engine_path, self.0.app_version.clone(), move |event| {
                        forward_protocol_event(emitter.as_ref(), event);
                    })
                    .await?;
                process
                    .load_model(Uuid::new_v4().to_string(), &model_component.path)
                    .await?;
                VoiceResult::Ok(process)
            }
            .await;
            let process = match started {
                Ok(process) => process,
                Err(error) => {
                    *self.0.engine_state.lock().await = EngineState::Failed;
                    emit_voice_event(
                        self.0.emitter.as_ref(),
                        VoiceEvent::EngineChanged {
                            state: EngineState::Failed,
                            detail: Some(error.to_string()),
                        },
                    );
                    return Err(error);
                }
            };
            *self.0.engine_state.lock().await = EngineState::Ready;
            emit_voice_event(
                self.0.emitter.as_ref(),
                VoiceEvent::EngineChanged {
                    state: EngineState::Ready,
                    detail: None,
                },
            );
            *guard = Some(process);
            self.0.restart_attempts.store(0, Ordering::Release);
            *self.0.loaded_model.lock().await = Some(model);
        } else if *self.0.loaded_model.lock().await != Some(model) {
            let manifest = self.0.installer.manifest().await?;
            let component = find_component(&manifest.components, model_kind(model))?;
            self.0.installer.verify(component).await?;
            guard
                .as_mut()
                .expect("checked above")
                .load_model(Uuid::new_v4().to_string(), &component.path)
                .await?;
            *self.0.loaded_model.lock().await = Some(model);
        }
        Ok(tokio::sync::MutexGuard::map(guard, |process| {
            process.as_mut().expect("voice process initialized above")
        }))
    }

    async fn invalidate_on_transport<T>(&self, result: &VoiceResult<T>) {
        if matches!(result, Err(VoiceError::Transport(_))) {
            self.recycle_process().await;
        }
    }

    async fn stop_process(&self) -> VoiceResult<()> {
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        let process = tokio::time::timeout_at(deadline, self.0.process.lock())
            .await
            .map_err(|_| {
                VoiceError::Transport("timed out acquiring voice process for shutdown".into())
            })?
            .take();
        if let Ok(mut loaded_model) = self.0.loaded_model.try_lock() {
            *loaded_model = None;
        }
        if let Ok(mut stream_state) = self.0.stream_state.try_lock() {
            *stream_state = StreamState::Idle;
        }
        if let Some(process) = process {
            process.shutdown_until(deadline).await?;
        }
        if let Ok(mut engine_state) = self.0.engine_state.try_lock() {
            *engine_state = EngineState::Stopped;
        }
        Ok(())
    }
}

/// The manifest kind a model is installed under.
fn model_kind(model: VoiceModel) -> &'static str {
    match model {
        VoiceModel::Fast => "model:fast",
        VoiceModel::Accurate => "model:accurate",
        VoiceModel::Realtime => "model:realtime",
    }
}

fn find_component<'a>(
    components: &'a [super::ManagedComponent],
    kind: &str,
) -> VoiceResult<&'a super::ManagedComponent> {
    components
        .iter()
        .find(|component| component.kind == kind)
        .ok_or(VoiceError::NotInstalled)
}

async fn component_state(
    installer: &ComponentInstaller,
    components: &[super::ManagedComponent],
    kind: &str,
) -> ComponentState {
    let Some(component) = components.iter().find(|component| component.kind == kind) else {
        return ComponentState::Missing;
    };
    match installer.verify(component).await {
        Ok(()) => ComponentState::Installed,
        Err(_) => ComponentState::Corrupt,
    }
}

fn emit_voice_event(emitter: &dyn EventEmitter, event: VoiceEvent) {
    if let Ok(payload) = serde_json::to_value(event) {
        let _ = emitter.emit(VOICE_EVENT_NAME, payload);
    }
}

fn forward_protocol_event(emitter: &dyn EventEmitter, event: ProtocolEvent) {
    match event {
        ProtocolEvent::Progress {
            request_id,
            stage,
            progress,
        } => emit_voice_event(
            emitter,
            VoiceEvent::TranscriptionProgress {
                request_id,
                stage,
                progress,
            },
        ),
        ProtocolEvent::Segment {
            request_id,
            index,
            start_ms,
            end_ms,
            text,
            ..
        } => emit_voice_event(
            emitter,
            VoiceEvent::Segment {
                request_id,
                segment: super::Segment {
                    index,
                    start_ms,
                    end_ms,
                    text,
                },
            },
        ),
        ProtocolEvent::Partial {
            stream_id,
            revision,
            window_start_ms,
            window_end_ms,
            text,
            ..
        } => emit_voice_event(
            emitter,
            VoiceEvent::Partial(super::StreamPartial {
                stream_id,
                revision,
                window_start_ms,
                window_end_ms,
                text,
            }),
        ),
        _ => {}
    }
}

/// Service-level tests against a scripted engine.
///
/// These are about who is allowed to use the engine and when — the questions
/// that only exist above the protocol, where a lost stream handle or a file job
/// arriving mid-dictation decides whether the next dictation works at all.
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::internal::services::testing::CapturingEmitter;
    use sha2::{Digest, Sha256};
    use std::os::unix::fs::PermissionsExt;

    const APP_VERSION: &str = "0.0.0-test";
    const BUILD_IDENTITY: &str = "content-v2:test-build";

    #[tokio::test]
    async fn stale_engine_is_incompatible_while_models_remain_installed() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let target = current_target().expect("voice tests run on a supported target");
        let make_component = |kind: &str, id: &str, bytes: &[u8]| {
            let path = root.join(id);
            std::fs::write(&path, bytes).unwrap();
            super::super::ManagedComponent {
                kind: kind.into(),
                id: id.into(),
                target: (kind == "engine").then(|| target.into()),
                size_bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(bytes)),
                path,
            }
        };
        let components = vec![
            make_component(
                "engine",
                &super::super::engine_asset_name("0.0.0-older", target),
                b"old engine",
            ),
            make_component("model:fast", "model-fast", b"unchanged model"),
            make_component("model:realtime", "model-realtime", b"unchanged gguf"),
        ];
        std::fs::write(
            root.join("voice-components.json"),
            serde_json::to_vec(&super::super::ManagedManifest {
                components: components.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        let service = VoiceService::with_engine(
            root.to_path_buf(),
            APP_VERSION,
            BUILD_IDENTITY,
            Arc::new(CapturingEmitter::new()),
            None,
        );
        let status = service.handle().status().await.unwrap();

        assert_eq!(
            status.engine,
            ComponentState::Incompatible,
            "status must not advertise an old app-version engine as installed"
        );
        assert_eq!(
            status.fast_model,
            ComponentState::Installed,
            "engine refresh eligibility must not invalidate version-independent models"
        );
        assert_eq!(
            status.realtime_model,
            ComponentState::Installed,
            "a Parakeet model is as version-independent as a whisper one"
        );
    }

    /// The engine every script starts from: it identifies itself, greets, logs
    /// each command to `$LOG`, and answers `loadModel` — a second late when a
    /// `$LOG.slow-load` marker exists, to stand in for a cold model.
    ///
    /// `$id` is the request's own correlation id, lifted out of the command.
    /// Answers carry it back because the supervisor discards any event that is
    /// not about the request it is waiting on — a fixed id would simply never
    /// arrive.
    const PREAMBLE: &str = r#"#!/bin/sh
LOG='@LOG@'
if [ "$1" = '--distribution-info' ]; then
  printf '{"name":"cairn-voice","version":"@VERSION@","protocolVersion":@PROTOCOL@}\n'
  exit 0
fi
printf '{"type":"hello","protocolVersion":@PROTOCOL@,"engineVersion":"@VERSION@"}\n'
while IFS= read -r command; do
  printf '%s\n' "$command" >> "$LOG"
  id=$(printf '%s' "$command" | sed -n 's/.*"requestId":"\([^"]*\)".*/\1/p')
  case "$command" in
    *'"type":"loadModel"'*)
      if [ -e "$LOG.slow-load" ]; then sleep 1; fi
      printf '{"type":"ready","requestId":"%s","model":{"path":"m","sizeBytes":1,"multilingual":true,"modelType":"test"}}\n' "$id" ;;
@SCRIPT@
  esac
done
"#;

    /// Opens and closes sessions, and transcribes files, all without complaint.
    const COOPERATIVE: &str = r#"    *'"type":"beginStream"'*)
      printf '{"type":"accepted","requestId":"%s"}\n' "$id" ;;
    *'"type":"endStream"'*)
      printf '{"type":"done","requestId":"%s","text":"","segments":[],"audioDurationMs":0}\n' "$id" ;;
    *'"type":"transcribeFile"'*)
      printf '{"type":"done","requestId":"%s","text":"a file","segments":[],"audioDurationMs":1}\n' "$id" ;;"#;

    /// An engine script that logs every command it receives to `log`.
    fn engine_script(log: &Path, script: &str) -> String {
        PREAMBLE
            .replace("@LOG@", &log.display().to_string())
            .replace("@VERSION@", APP_VERSION)
            .replace("@PROTOCOL@", &crate::voice::PROTOCOL_VERSION.to_string())
            .replace("@SCRIPT@", script)
    }

    fn scripted_engine(root: &Path, script: &str) -> PathBuf {
        let executable = root.join("fake-voice-engine");
        std::fs::write(
            &executable,
            engine_script(&root.join("commands.log"), script),
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        executable
    }

    /// Managed model components the installer will actually verify: real files
    /// under the managed root, with real sizes and digests.
    fn manifest(root: &Path) -> Value {
        std::fs::create_dir_all(root.join("models")).unwrap();
        let components: Vec<Value> = ["model:fast", "model:accurate"]
            .iter()
            .map(|kind| {
                let id = kind.replace(':', "-");
                let path = root.join("models").join(&id);
                std::fs::write(&path, kind.as_bytes()).unwrap();
                serde_json::json!({
                    "kind": kind,
                    "id": id,
                    "target": Value::Null,
                    "sizeBytes": kind.len(),
                    "sha256": format!("{:x}", Sha256::digest(kind.as_bytes())),
                    "path": path,
                })
            })
            .collect();
        serde_json::json!({ "components": components })
    }

    struct Harness {
        directory: tempfile::TempDir,
        service: VoiceService,
    }

    impl Harness {
        /// A service running `script` through the development override.
        fn new(script: &str) -> Harness {
            Harness::with_preferences(
                script,
                super::super::VoicePreferences {
                    enabled: true,
                    ..Default::default()
                },
            )
        }

        fn with_preferences(script: &str, preferences: super::super::VoicePreferences) -> Harness {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().to_path_buf();
            let engine = scripted_engine(&root, script);
            std::fs::write(
                root.join("voice-components.json"),
                serde_json::to_vec(&manifest(&root)).unwrap(),
            )
            .unwrap();
            Self::start(directory, preferences, Some(engine))
        }

        /// A service running `script` as its managed engine, the way a
        /// production app does: published under its release asset name and
        /// recorded in the manifest, with no override.
        fn managed(script: &str) -> Harness {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().to_path_buf();
            let target = current_target().expect("voice tests run on a supported target");
            let asset = super::super::engine_asset_name(APP_VERSION, target);
            let engine_path = root.join("voice/engine").join(&asset);
            std::fs::create_dir_all(engine_path.parent().unwrap()).unwrap();
            let body = engine_script(&root.join("commands.log"), script);
            std::fs::write(&engine_path, &body).unwrap();
            std::fs::set_permissions(&engine_path, std::fs::Permissions::from_mode(0o755)).unwrap();
            let mut manifest = manifest(&root);
            manifest["components"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "kind": "engine",
                    "id": asset,
                    "target": target,
                    "sizeBytes": body.len(),
                    "sha256": format!("{:x}", Sha256::digest(body.as_bytes())),
                    "path": engine_path,
                }));
            std::fs::write(
                root.join("voice-components.json"),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            Self::start(
                directory,
                super::super::VoicePreferences {
                    enabled: true,
                    ..Default::default()
                },
                None,
            )
        }

        fn start(
            directory: tempfile::TempDir,
            preferences: super::super::VoicePreferences,
            engine_override: Option<PathBuf>,
        ) -> Harness {
            let root = directory.path().to_path_buf();
            // Enabled through the settings file rather than through `enable()`,
            // which downloads.
            crate::config::settings::set_voice_preferences(&root, &preferences).unwrap();
            Harness {
                service: VoiceService::with_engine(
                    root,
                    APP_VERSION,
                    BUILD_IDENTITY,
                    Arc::new(CapturingEmitter::new()),
                    engine_override,
                ),
                directory,
            }
        }

        /// Hand off an engine running `script` to this runner, as a verified
        /// self-rebuild does. It logs to `adopted.log`, so whichever engine
        /// served a command is visible.
        fn hand_off(&self, script: &str) {
            let root = self.directory.path();
            let target = current_target().expect("voice tests run on a supported target");
            let asset = super::super::engine_asset_name(APP_VERSION, target);
            let dir = root.join("voice/local-engine");
            std::fs::create_dir_all(&dir).unwrap();
            let body = engine_script(&root.join("adopted.log"), script);
            std::fs::write(dir.join(&asset), &body).unwrap();
            std::fs::write(
                dir.join(format!("{asset}.sha256")),
                format!("{:x}  {asset}\n", Sha256::digest(body.as_bytes())),
            )
            .unwrap();
            std::fs::write(
                dir.join("handoff.json"),
                serde_json::json!({ "buildId": BUILD_IDENTITY, "asset": asset }).to_string(),
            )
            .unwrap();
        }

        /// Every command line the handed-off engine has received.
        fn adopted_commands(&self) -> Vec<String> {
            std::fs::read_to_string(self.directory.path().join("adopted.log"))
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect()
        }

        fn voice(&self) -> VoiceServiceHandle {
            self.service.handle()
        }

        /// Every command line the engine has received, in order.
        fn commands(&self) -> Vec<String> {
            std::fs::read_to_string(self.directory.path().join("commands.log"))
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect()
        }

        /// How many commands of one kind the engine has received, across every
        /// process that has served this harness.
        fn count_command(&self, kind: &str) -> usize {
            self.commands()
                .iter()
                .filter(|line| line.contains(&format!("\"type\":\"{kind}\"")))
                .count()
        }

        /// Every `loadModel` the engine has received, as the model files named.
        fn loads(&self) -> Vec<String> {
            self.commands()
                .into_iter()
                .filter(|line| line.contains("\"type\":\"loadModel\""))
                .map(|line| {
                    ["model-fast", "model-accurate"]
                        .into_iter()
                        .find(|model| line.contains(model))
                        .unwrap_or("unknown")
                        .to_string()
                })
                .collect()
        }

        /// Make every `loadModel` take a second, as a cold model does.
        fn slow_loads(&self) {
            std::fs::write(self.directory.path().join("commands.log.slow-load"), b"").unwrap();
        }

        /// Wait for background work to reach the engine.
        async fn wait_for_loads(&self, count: usize) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while self.loads().len() < count {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "expected {count} model loads, saw {:?}",
                    self.loads()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        /// The most recent command of one kind the engine received.
        fn last_command(&self, kind: &str) -> String {
            self.commands()
                .into_iter()
                .rev()
                .find(|line| line.contains(&format!("\"type\":\"{kind}\"")))
                .unwrap_or_else(|| panic!("no {kind} command was sent"))
        }
    }

    /// The pin the issue asks for: the selector must actually decide.
    #[tokio::test]
    async fn a_live_session_opens_with_the_model_dictation_is_set_to() {
        let harness = Harness::new(COOPERATIVE);
        let voice = harness.voice();

        let first = voice.stream_start().await.expect("session opens");
        voice.stream_finish(first).await.expect("session closes");
        assert!(
            harness.last_command("loadModel").contains("model-fast"),
            "dictation defaults to Fast: {:?}",
            harness.last_command("loadModel")
        );

        voice
            .set_preferred_model(ModelPurpose::Dictation, VoiceModel::Accurate)
            .await
            .expect("the preference is accepted");
        let second = voice.stream_start().await.expect("session opens");
        voice.stream_finish(second).await.expect("session closes");

        assert!(
            harness.last_command("loadModel").contains("model-accurate"),
            "the next session must open on the chosen model: {:?}",
            harness.last_command("loadModel")
        );
        assert_eq!(
            voice.preferred_file_model().await,
            VoiceModel::Fast,
            "choosing a dictation model must not move the file setting"
        );
    }

    /// The wedge: dictation must survive a session nobody closed.
    ///
    /// A webview that reloads mid-stream, or a flush that throws, destroys the
    /// only handle the caller had. The engine still holds that session and
    /// refuses every `beginStream` after it — with a `Request` error, which
    /// never recycles the process — so dictation stayed dead until the app
    /// restarted. This service minted the id, so it can always close it.
    #[tokio::test]
    async fn a_stream_nobody_closed_does_not_wedge_the_next_dictation() {
        let harness = Harness::new(COOPERATIVE);
        let voice = harness.voice();

        let abandoned = voice.stream_start().await.expect("first session opens");
        // No finish, no cancel: exactly what a reloaded webview leaves behind.
        let replacement = voice.stream_start().await.expect("dictation still works");
        assert_ne!(abandoned, replacement);

        let commands = harness.commands();
        let closed = commands
            .iter()
            .position(|line| line.contains("endStream") && line.contains(&abandoned))
            .expect("the abandoned session must be closed");
        let reopened = commands
            .iter()
            .position(|line| line.contains("beginStream") && line.contains(&replacement))
            .expect("the replacement session must be opened");
        assert!(
            closed < reopened,
            "the stale session must be closed before the new one opens: {commands:?}"
        );
    }

    /// Defence in depth for a divergence this service cannot otherwise explain.
    #[tokio::test]
    async fn a_stream_already_active_refusal_rebuilds_the_engine() {
        // Refuses the first `beginStream` and accepts every one after. The
        // marker outlives the process, so the retry can only succeed if a fresh
        // engine was spawned to serve it.
        let harness = Harness::new(
            r#"    *'"type":"beginStream"'*)
      if [ -e "$LOG.refused" ]; then
        printf '{"type":"accepted","requestId":"%s"}\n' "$id"
      else
        : > "$LOG.refused"
        printf '{"type":"error","requestId":"%s","code":"stream_already_active","message":"a stream is already open"}\n' "$id"
      fi ;;"#,
        );
        let voice = harness.voice();

        let error = voice
            .stream_start()
            .await
            .expect_err("the engine refuses the first attempt");
        assert!(
            matches!(&error, VoiceError::Request { code, .. } if code == "stream_already_active"),
            "{error:?}"
        );
        // A `Request` error never recycles the process on its own. Unless the
        // service rebuilds the engine here, dictation is over for the life of
        // the runner.
        voice
            .stream_start()
            .await
            .expect("the next attempt gets a clean engine");
    }

    /// The engine services one thing at a time, so say so.
    #[tokio::test]
    async fn a_file_transcription_is_declined_while_dictation_is_live() {
        let harness = Harness::new(COOPERATIVE);
        let voice = harness.voice();
        let stream = voice.stream_start().await.expect("session opens");

        let error = voice
            .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
            .await
            .expect_err("a file job must not run under a live session");
        assert!(matches!(error, VoiceError::Busy), "{error:?}");

        // Declining is not the same as breaking: closing the session frees it.
        voice.stream_finish(stream).await.expect("session finishes");
        voice
            .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
            .await
            .expect("the engine is free once the session is closed");
    }

    /// The other direction, which used to park instead of declining.
    ///
    /// A file job started mid-dictation held the process for its whole
    /// duration. The live session's audio queued behind it while the UI's
    /// bounded capture buffer discarded the front of the sentence to stay
    /// bounded, and the session could not even be closed inside the desktop
    /// proxy's timeout.
    #[tokio::test]
    async fn dictation_is_declined_while_a_file_transcription_is_in_flight() {
        let harness = Harness::new(
            r#"    *'"type":"beginStream"'*)
      printf '{"type":"accepted","requestId":"%s"}\n' "$id" ;;
    *'"type":"endStream"'*)
      printf '{"type":"done","requestId":"%s","text":"","segments":[],"audioDurationMs":0}\n' "$id" ;;
    *'"type":"transcribeFile"'*)
      sleep 1
      printf '{"type":"done","requestId":"%s","text":"a file","segments":[],"audioDurationMs":1}\n' "$id" ;;"#,
        );
        let voice = harness.voice();

        let transcribing = {
            let voice = voice.clone();
            tokio::spawn(async move {
                voice
                    .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
                    .await
            })
        };
        // Long enough for the file job to claim the engine, well short of the
        // second it holds it for.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let error = voice
            .stream_start()
            .await
            .expect_err("dictation must decline rather than park behind the file");
        assert!(matches!(error, VoiceError::Busy), "{error:?}");

        transcribing.await.unwrap().expect("the file job completes");
        voice
            .stream_start()
            .await
            .expect("dictation works again once the engine is free");
    }

    /// A session being closed is not a session that is gone.
    ///
    /// The engine has not said it is done with it yet, and the close is holding
    /// the process while it asks. Admitting anyone on the strength of a cleared
    /// handle would park them behind exactly the work the decline exists to
    /// keep them away from.
    #[tokio::test]
    async fn nothing_is_admitted_while_a_close_is_still_in_flight() {
        let harness = Harness::new(
            r#"    *'"type":"beginStream"'*)
      printf '{"type":"accepted","requestId":"%s"}\n' "$id" ;;
    *'"type":"endStream"'*)
      sleep 1
      printf '{"type":"done","requestId":"%s","text":"","segments":[],"audioDurationMs":0}\n' "$id" ;;
    *'"type":"transcribeFile"'*)
      printf '{"type":"done","requestId":"%s","text":"a file","segments":[],"audioDurationMs":1}\n' "$id" ;;"#,
        );
        let voice = harness.voice();
        let stream = voice.stream_start().await.expect("session opens");

        let closing = {
            let voice = voice.clone();
            tokio::spawn(async move { voice.stream_finish(stream).await })
        };
        // Inside the second the engine takes to answer the close.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let file = voice
            .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
            .await
            .expect_err("a file job must not be admitted mid-close");
        assert!(matches!(file, VoiceError::Busy), "{file:?}");
        let dictation = voice
            .stream_start()
            .await
            .expect_err("a session must not open onto an engine still closing one");
        assert!(matches!(dictation, VoiceError::Busy), "{dictation:?}");

        closing.await.unwrap().expect("the close completes");

        // And the decline was about this moment, not a permanent state.
        voice
            .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
            .await
            .expect("the engine is free once the close lands");
    }

    fn dictating_with(model: VoiceModel) -> super::super::VoicePreferences {
        super::super::VoicePreferences {
            enabled: true,
            preferred_dictation_model: model,
            ..Default::default()
        }
    }

    /// The first press after app start must not pay for the model load.
    #[tokio::test]
    async fn startup_warms_the_dictation_model_before_anyone_dictates() {
        let harness = Harness::with_preferences(COOPERATIVE, dictating_with(VoiceModel::Accurate));
        let voice = harness.voice();

        harness.service.start_background_refresh();
        harness.wait_for_loads(1).await;
        assert_eq!(
            harness.loads(),
            ["model-accurate"],
            "the warm-up loads the model dictation is set to, not the file one"
        );

        let stream = voice.stream_start().await.expect("session opens");
        voice.stream_finish(stream).await.expect("session closes");
        assert_eq!(
            harness.loads(),
            ["model-accurate"],
            "a dictation after the warm-up opens on the loaded model"
        );
    }

    /// A press during the warm-up joins it rather than racing it.
    #[tokio::test]
    async fn a_dictation_pressed_mid_warm_up_waits_on_the_same_load() {
        let harness = Harness::with_preferences(COOPERATIVE, dictating_with(VoiceModel::Accurate));
        harness.slow_loads();
        let voice = harness.voice();

        voice.prewarm_dictation();
        // Inside the second the load takes.
        harness.wait_for_loads(1).await;
        let stream = voice
            .stream_start()
            .await
            .expect("the session opens once the load lands");
        voice.stream_finish(stream).await.expect("session closes");

        assert_eq!(
            harness.loads(),
            ["model-accurate"],
            "the session must reuse the warm-up's load, not start a second one"
        );
        let commands = harness.commands();
        let loaded = commands
            .iter()
            .position(|line| line.contains("\"type\":\"loadModel\""))
            .unwrap();
        let began = commands
            .iter()
            .position(|line| line.contains("\"type\":\"beginStream\""))
            .unwrap();
        assert!(loaded < began, "{commands:?}");
    }

    /// A warm-up is opportunistic: it never parks behind real work, and never
    /// swaps the model out from under it.
    #[tokio::test]
    async fn a_warm_up_declines_while_a_file_transcription_owns_the_engine() {
        let harness = Harness::with_preferences(
            r#"    *'"type":"transcribeFile"'*)
      sleep 1
      printf '{"type":"done","requestId":"%s","text":"a file","segments":[],"audioDurationMs":1}\n' "$id" ;;"#,
            dictating_with(VoiceModel::Accurate),
        );
        let voice = harness.voice();
        let transcribing = {
            let voice = voice.clone();
            tokio::spawn(async move {
                voice
                    .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
                    .await
            })
        };
        harness.wait_for_loads(1).await;

        let started = tokio::time::Instant::now();
        assert!(
            !voice.warm_dictation_model().await,
            "the warm-up must decline under a file job"
        );
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "declining must not wait for the file job"
        );

        transcribing.await.unwrap().expect("the file job completes");
        assert_eq!(harness.loads(), ["model-fast"]);
    }

    /// The race the admission check alone left open: work that registers after
    /// the idle check and reaches the engine first. Holding the process lock
    /// stands in for it deterministically. Waiting here would park the
    /// warm-up behind the whole of that work.
    #[tokio::test]
    async fn a_warm_up_never_waits_for_an_engine_someone_else_holds() {
        let harness = Harness::new(COOPERATIVE);
        let voice = harness.voice();
        let held = voice.0.process.lock().await;

        let warmed = tokio::time::timeout(Duration::from_millis(500), voice.warm_dictation_model())
            .await
            .expect("the warm-up must decline, not queue");
        assert!(!warmed);
        drop(held);
        assert!(harness.loads().is_empty(), "{:?}", harness.loads());
    }

    /// The other side of the claim: work that arrives once a warm-up holds the
    /// engine is served after the load, not refused or raced.
    #[tokio::test]
    async fn a_file_job_arriving_mid_warm_up_runs_after_the_load() {
        let harness = Harness::with_preferences(COOPERATIVE, dictating_with(VoiceModel::Accurate));
        harness.slow_loads();
        let voice = harness.voice();

        voice.prewarm_dictation();
        harness.wait_for_loads(1).await;
        voice
            .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
            .await
            .expect("the file job runs once the warm-up's load lands");
        assert_eq!(harness.loads(), ["model-accurate", "model-fast"]);
    }

    /// A disable or shutdown that lands while a load is waiting for the process
    /// must win: the loader rechecks under the lock, so nothing spawns an
    /// engine that the already-finished stop will never reclaim.
    #[tokio::test]
    async fn a_load_overtaken_by_disable_or_shutdown_spawns_nothing() {
        let harness = Harness::new(COOPERATIVE);
        let voice = harness.voice();

        let guard = voice.0.process.lock().await;
        voice.0.enabled.store(false, Ordering::Release);
        let disabled = voice.load_into(guard, VoiceModel::Fast).await.map(drop);
        assert!(
            matches!(disabled, Err(VoiceError::NotInstalled)),
            "{disabled:?}"
        );

        voice.0.enabled.store(true, Ordering::Release);
        let guard = voice.0.process.lock().await;
        voice.begin_shutdown();
        let shut_down = voice.load_into(guard, VoiceModel::Fast).await.map(drop);
        assert!(
            matches!(shut_down, Err(VoiceError::Transport(_))),
            "{shut_down:?}"
        );

        assert!(voice.0.process.lock().await.is_none());
        assert!(harness.commands().is_empty(), "{:?}", harness.commands());
    }

    #[tokio::test]
    async fn a_warm_up_declines_while_a_session_is_open() {
        let harness = Harness::new(COOPERATIVE);
        let voice = harness.voice();
        let stream = voice.stream_start().await.expect("session opens");

        assert!(!voice.warm_dictation_model().await);
        voice.stream_finish(stream).await.expect("session closes");
        assert_eq!(harness.loads(), ["model-fast"]);
    }

    /// Choosing a different dictation model warms that one, so the next press
    /// does not pay for the switch.
    #[tokio::test]
    async fn choosing_a_dictation_model_warms_it() {
        let harness = Harness::new(COOPERATIVE);
        let voice = harness.voice();
        assert!(voice.warm_dictation_model().await);
        assert_eq!(harness.loads(), ["model-fast"]);

        voice
            .set_preferred_model(ModelPurpose::Dictation, VoiceModel::Accurate)
            .await
            .expect("the preference is accepted");
        harness.wait_for_loads(2).await;
        assert_eq!(harness.loads(), ["model-fast", "model-accurate"]);

        let stream = voice.stream_start().await.expect("session opens");
        voice.stream_finish(stream).await.expect("session closes");
        assert_eq!(
            harness.loads(),
            ["model-fast", "model-accurate"],
            "the session opens on the model the change already warmed"
        );

        // Re-choosing the same model, and choosing a file model, warm nothing.
        voice
            .set_preferred_model(ModelPurpose::Dictation, VoiceModel::Accurate)
            .await
            .unwrap();
        voice
            .set_preferred_model(ModelPurpose::File, VoiceModel::Fast)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(harness.loads(), ["model-fast", "model-accurate"]);
    }

    #[tokio::test]
    async fn a_disabled_voice_stays_cold() {
        let harness = Harness::with_preferences(
            COOPERATIVE,
            super::super::VoicePreferences {
                enabled: false,
                ..Default::default()
            },
        );

        harness.service.start_background_refresh();
        assert!(!harness.voice().warm_dictation_model().await);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(harness.commands().is_empty(), "{:?}", harness.commands());
    }

    /// A close that fails leaves the engine possibly still holding the session,
    /// so the handle cannot simply be dropped: the process is recycled first,
    /// and only a genuinely fresh engine is advertised as free.
    #[tokio::test]
    async fn a_close_that_fails_recycles_the_engine_before_admitting_anyone() {
        let harness = Harness::new(
            r#"    *'"type":"beginStream"'*)
      printf '{"type":"accepted","requestId":"%s"}\n' "$id" ;;
    *'"type":"endStream"'*)
      printf '{"type":"error","requestId":"%s","code":"unknown_stream","message":"no such stream"}\n' "$id" ;;
    *'"type":"transcribeFile"'*)
      printf '{"type":"done","requestId":"%s","text":"a file","segments":[],"audioDurationMs":1}\n' "$id" ;;"#,
        );
        let voice = harness.voice();

        let stream = voice.stream_start().await.expect("session opens");
        let loads_before = harness.count_command("loadModel");
        voice
            .stream_finish(stream)
            .await
            .expect_err("the engine refuses the close");

        // Admitted, because the engine that might still hold the old session
        // has been replaced rather than merely forgotten — a second `loadModel`
        // is a second process.
        voice
            .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
            .await
            .expect("a fresh engine serves the next request");
        assert!(
            harness.count_command("loadModel") > loads_before,
            "a failed close must recycle the process, not just drop the handle: {:?}",
            harness.commands()
        );
    }

    /// A self-rebuild hands off the engine built from the same revision as the
    /// app; the next dictation must run on it rather than on the engine the
    /// manifest already called current under the same asset name.
    #[tokio::test]
    async fn the_next_dictation_runs_on_a_handed_off_engine() {
        let harness = Harness::managed(COOPERATIVE);
        harness.hand_off(COOPERATIVE);
        let voice = harness.voice();

        let stream = voice.stream_start().await.expect("session opens");
        voice.stream_finish(stream).await.expect("session closes");

        let adopted = harness.adopted_commands();
        assert!(
            adopted
                .iter()
                .any(|line| line.contains("\"type\":\"beginStream\"")),
            "the handed-off engine serves the session: {adopted:?}"
        );
        assert!(
            harness.commands().is_empty(),
            "the superseded engine never runs: {:?}",
            harness.commands()
        );
        assert!(!harness.directory.path().join("voice/local-engine").exists());
        assert_eq!(
            voice.status().await.unwrap().engine,
            ComponentState::Installed,
            "the adopted engine verifies against what the manifest recorded"
        );
    }

    /// A handoff that lands while the runner already has a live engine retires
    /// that process at the next unit of work.
    #[tokio::test]
    async fn a_live_engine_gives_way_to_a_handoff_at_the_next_request() {
        let harness = Harness::managed(COOPERATIVE);
        let voice = harness.voice();
        let stream = voice.stream_start().await.expect("session opens");
        voice.stream_finish(stream).await.expect("session closes");
        assert_eq!(harness.count_command("loadModel"), 1);

        harness.hand_off(COOPERATIVE);
        voice
            .transcribe_file(Path::new("/tmp/whatever.wav"), VoiceModel::Fast, None)
            .await
            .expect("the adopted engine serves the file");

        let adopted = harness.adopted_commands();
        assert!(
            adopted
                .iter()
                .any(|line| line.contains("\"type\":\"loadModel\""))
                && adopted
                    .iter()
                    .any(|line| line.contains("\"type\":\"transcribeFile\"")),
            "a fresh process of the handed-off engine serves the request: {adopted:?}"
        );
        assert_eq!(
            harness.count_command("transcribeFile"),
            0,
            "the superseded process is not reused"
        );
    }
}
