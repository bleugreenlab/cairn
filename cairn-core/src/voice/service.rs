use super::{
    current_target, model_catalog, ComponentInstaller, ComponentState, DictationMode, EngineState,
    InstallEvent, ProtocolCommand, ProtocolEvent, Transcript, VoiceError, VoiceEvent, VoiceModel,
    VoiceProcess, VoiceResult, VoiceStatus,
};
use crate::services::EventEmitter;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

const RELEASE_BASE: &str = "https://github.com/bleugreenlab/cairn/releases/download";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
pub const VOICE_EVENT_NAME: &str = "voice-event";

struct VoiceServiceInner {
    installer: ComponentInstaller,
    config_dir: PathBuf,
    app_version: String,
    engine_override: Option<PathBuf>,
    enabled: AtomicBool,
    preferred_file_model: Mutex<VoiceModel>,
    dictation_mode: Mutex<DictationMode>,
    input_device_id: Mutex<Option<String>>,
    loaded_model: Mutex<Option<VoiceModel>>,
    shutting_down: AtomicBool,
    process: Mutex<Option<VoiceProcess>>,
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
    pub fn new(
        data_dir: PathBuf,
        app_version: impl Into<String>,
        emitter: Arc<dyn EventEmitter>,
    ) -> Self {
        let app_version = app_version.into();
        // This escape hatch exists only in development/test builds. Production
        // always executes a checksum-pinned managed release artifact.
        let engine_override = cfg!(debug_assertions)
            .then(|| std::env::var_os("CAIRN_VOICE_ENGINE_PATH").map(PathBuf::from))
            .flatten();
        let preferences = crate::config::settings::load_voice_preferences(&data_dir);
        let install_emitter = emitter.clone();
        let installer = ComponentInstaller::new(
            data_dir.clone(),
            RELEASE_BASE,
            app_version.clone(),
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
                dictation_mode: Mutex::new(preferences.dictation_mode),
                input_device_id: Mutex::new(preferences.input_device_id),
                loaded_model: Mutex::new(None),
                shutting_down: AtomicBool::new(false),
                process: Mutex::new(None),
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

    pub fn begin_shutdown(&self) {
        self.handle.0.shutting_down.store(true, Ordering::Release);
    }

    pub async fn shutdown(self) {
        self.begin_shutdown();
        let _ = self.handle.stop_process().await;
    }
}

impl VoiceServiceHandle {
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
            component_state(&self.0.installer, &manifest.components, "engine").await
        };
        let fast_model =
            component_state(&self.0.installer, &manifest.components, "model:fast").await;
        let accurate_model =
            component_state(&self.0.installer, &manifest.components, "model:accurate").await;
        Ok(VoiceStatus {
            supported: current_target().is_some(),
            enabled: self.0.enabled.load(Ordering::Acquire),
            engine,
            engine_state: self.0.engine_state.lock().await.clone(),
            fast_model,
            accurate_model,
            preferred_file_model: *self.0.preferred_file_model.lock().await,
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
        self.status().await
    }

    pub async fn set_preferred_file_model(&self, model: VoiceModel) -> VoiceResult<VoiceStatus> {
        let manifest = self.0.installer.manifest().await?;
        let kind = match model {
            VoiceModel::Fast => "model:fast",
            VoiceModel::Accurate => "model:accurate",
        };
        self.0
            .installer
            .verify(find_component(&manifest.components, kind)?)
            .await?;
        let mut preferences = self
            .preferences(self.0.enabled.load(Ordering::Acquire))
            .await;
        preferences.preferred_file_model = model;
        self.persist_preferences(preferences)?;
        *self.0.preferred_file_model.lock().await = model;
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

    pub async fn transcribe_file(&self, path: &Path, model: VoiceModel) -> VoiceResult<Transcript> {
        let request_id = Uuid::new_v4().to_string();
        let process = self.process_for(model).await?;
        let result = process
            .request(
                &ProtocolCommand::TranscribeFile {
                    request_id: request_id.clone(),
                    path: path.to_string_lossy().into_owned(),
                    options: Value::Object(Default::default()),
                },
                &request_id,
            )
            .await;
        drop(process);
        self.invalidate_on_transport(&result).await;
        result
    }

    pub async fn stream_start(&self) -> VoiceResult<String> {
        let stream_id = Uuid::new_v4().to_string();
        let request_id = Uuid::new_v4().to_string();
        let process = self.process_for(VoiceModel::Fast).await?;
        let result = process
            .command(
                &ProtocolCommand::BeginStream {
                    request_id: request_id.clone(),
                    stream_id: stream_id.clone(),
                    options: Value::Object(Default::default()),
                },
                &request_id,
            )
            .await;
        drop(process);
        self.invalidate_on_transport(&result).await;
        result?;
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
        let request_id = Uuid::new_v4().to_string();
        let mut guard = self.0.process.lock().await;
        let process = guard.as_mut().ok_or(VoiceError::NotInstalled)?;
        let result = process
            .request(
                &ProtocolCommand::EndStream {
                    request_id: request_id.clone(),
                    stream_id,
                },
                &request_id,
            )
            .await;
        drop(guard);
        self.invalidate_on_transport(&result).await;
        result
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
        self.ensure_available()?;
        if !self.0.enabled.load(Ordering::Acquire) {
            return Err(VoiceError::NotInstalled);
        }
        let mut guard = self.0.process.lock().await;
        let process_crashed = match guard.as_ref() {
            Some(process) => !process.is_running().await,
            None => false,
        };
        if process_crashed {
            guard.take();
            *self.0.loaded_model.lock().await = None;
            *self.0.engine_state.lock().await = EngineState::Restarting;
            let attempt = self
                .0
                .restart_attempts
                .fetch_add(1, Ordering::AcqRel)
                .min(4);
            tokio::time::sleep(std::time::Duration::from_millis(100 * (1_u64 << attempt))).await;
        }
        if guard.is_none() {
            let manifest = self.0.installer.manifest().await?;
            let engine_path = if let Some(path) = &self.0.engine_override {
                super::installer::probe_engine(path, &self.0.app_version).await?;
                path.clone()
            } else {
                let engine = find_component(&manifest.components, "engine")?;
                self.0.installer.verify(engine).await?;
                engine.path.clone()
            };
            let model_kind = match model {
                VoiceModel::Fast => "model:fast",
                VoiceModel::Accurate => "model:accurate",
            };
            let model_component = find_component(&manifest.components, model_kind)?;
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
            let model_kind = match model {
                VoiceModel::Fast => "model:fast",
                VoiceModel::Accurate => "model:accurate",
            };
            let component = find_component(&manifest.components, model_kind)?;
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
            self.0.process.lock().await.take();
            *self.0.loaded_model.lock().await = None;
            *self.0.engine_state.lock().await = EngineState::Restarting;
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
        if let Some(process) = process {
            process.shutdown_until(deadline).await?;
        }
        if let Ok(mut engine_state) = self.0.engine_state.try_lock() {
            *engine_state = EngineState::Stopped;
        }
        Ok(())
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
