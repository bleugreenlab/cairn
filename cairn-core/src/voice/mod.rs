//! Desktop voice domain contract.
//!
//! This module owns voice intent and the immutable component catalog. Runtime
//! state is derived from verified managed files; it is never persisted as fact
//! in `settings.yaml`.

mod catalog;
mod service;

pub use service::{VoiceService, VoiceServiceHandle};
mod installer;
mod supervisor;

pub use catalog::{current_target, engine_asset_name, model_catalog, ModelArtifact};
pub use installer::{ComponentInstaller, InstallEvent, ManagedManifest};
pub use supervisor::{ProtocolCommand, ProtocolEvent, VoiceProcess};

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const PROTOCOL_VERSION: u32 = 1;
pub const SAMPLE_RATE: u32 = 16_000;
pub const CHANNELS: u32 = 1;
pub const CHUNK_ENCODING: &str = "f32le-base64";
pub const MAX_AUDIO_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoiceModel {
    #[default]
    Fast,
    Accurate,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DictationMode {
    #[default]
    Hold,
    Toggle,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoicePreferences {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub preferred_file_model: VoiceModel,
    #[serde(default)]
    pub dictation_mode: DictationMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentState {
    Missing,
    Downloading,
    Installed,
    Corrupt,
    Incompatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineState {
    Stopped,
    Starting,
    Ready,
    Busy,
    Restarting,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoiceStatus {
    pub supported: bool,
    pub enabled: bool,
    pub engine: ComponentState,
    pub engine_state: EngineState,
    pub fast_model: ComponentState,
    pub accurate_model: ComponentState,
    pub preferred_file_model: VoiceModel,
    pub dictation_mode: DictationMode,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    pub index: u32,
    pub start_ms: i64,
    pub end_ms: i64,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transcript {
    pub text: String,
    pub segments: Vec<Segment>,
    pub audio_duration_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamPartial {
    pub stream_id: String,
    pub revision: u64,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum VoiceEvent {
    DownloadStarted {
        operation_id: String,
        component: String,
        total_bytes: Option<u64>,
    },
    DownloadProgress {
        operation_id: String,
        component: String,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    DownloadFinished {
        operation_id: String,
        component: String,
    },
    EngineChanged {
        state: EngineState,
        detail: Option<String>,
    },
    TranscriptionProgress {
        request_id: String,
        stage: String,
        progress: f32,
    },
    Segment {
        request_id: String,
        segment: Segment,
    },
    Partial(StreamPartial),
    StreamTerminated {
        stream_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedComponent {
    pub kind: String,
    pub id: String,
    pub target: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
    pub path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum VoiceError {
    #[error("voice is not supported on this target")]
    UnsupportedTarget,
    #[error("voice component is not installed")]
    NotInstalled,
    #[error("voice component failed verification: {0}")]
    Corrupt(String),
    #[error("voice engine is incompatible: {0}; remove and re-download voice files")]
    Incompatible(String),
    #[error("voice engine is busy")]
    Busy,
    #[error("voice command queue is full; retry after pending audio is processed")]
    Backpressure,
    #[error("invalid voice input: {0}")]
    InvalidInput(String),
    #[error("voice request failed ({code}): {message}")]
    Request { code: String, message: String },
    #[error("voice transport failed: {0}")]
    Transport(String),
    #[error("voice installation failed: {0}")]
    Install(String),
}

pub type VoiceResult<T> = Result<T, VoiceError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferences_default_to_disabled_fast_model_and_hold_dictation() {
        assert_eq!(
            VoicePreferences::default(),
            VoicePreferences {
                enabled: false,
                preferred_file_model: VoiceModel::Fast,
                dictation_mode: DictationMode::Hold,
            }
        );
    }

    #[test]
    fn legacy_preferences_default_dictation_mode_to_hold() {
        let preferences: VoicePreferences =
            serde_yaml::from_str("enabled: true\npreferredFileModel: accurate\n").unwrap();
        assert_eq!(preferences.dictation_mode, DictationMode::Hold);
    }
}
